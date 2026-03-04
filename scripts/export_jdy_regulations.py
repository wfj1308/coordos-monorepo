#!/usr/bin/env python3
"""
Export regulation rows from a public JianDaoYun dashboard to migration CSV.

Usage:
  python scripts/export_jdy_regulations.py \
    --dash-url "https://xxx.jiandaoyun.com/dash/<id>" \
    --output "scripts/regulations_jdy_export.csv"
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


DEFAULT_DASH_URL = "https://tnwwzyqy43.jiandaoyun.com/dash/6329a1a6aaf3e400095a23ad"


def _pick_first_component(components: Dict[str, Any], comp_type: str) -> Tuple[str, Dict[str, Any]]:
    for widget_id, comp in components.items():
        if isinstance(comp, dict) and comp.get("type") == comp_type:
            return widget_id, comp
    raise RuntimeError(f"no component with type={comp_type!r} found in dashboard")


def _first_match_field_name(fields: List[Dict[str, Any]], keywords: List[str], field_type: Optional[str] = None) -> str:
    for f in fields:
        title = str(f.get("title") or "")
        ftype = str(f.get("type") or "")
        if field_type and ftype != field_type:
            continue
        if any(k in title for k in keywords):
            return str(f.get("name") or "")
    return ""


def _normalize_status(v: Any) -> str:
    text = str(v or "").strip()
    if not text:
        return "EFFECTIVE"
    if any(k in text for k in ["废止", "作废", "失效", "废"]):
        return "REPEALED"
    if any(k in text for k in ["无效", "过期", "终止"]):
        return "EXPIRED"
    return "EFFECTIVE"


def _to_date(v: Any) -> str:
    if not v:
        return ""
    return str(v)[:10]


def _clip(v: Any, limit: int) -> str:
    text = str(v or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit]


def _first_attachment_url(v: Any) -> str:
    if not isinstance(v, list) or not v:
        return ""
    item = v[0] or {}
    return str(item.get("downloadUrl") or item.get("url") or item.get("previewUrl") or "")


def _extract_auth_from_html(html: str) -> Dict[str, str]:
    def must_find(pattern: str, name: str) -> str:
        m = re.search(pattern, html, re.S)
        if not m:
            raise RuntimeError(f"cannot find {name} in dashboard html")
        return m.group(1)

    return {
        "csrf": must_find(r'window\.jdy_csrf_token\s*=\s*"([^"]+)"', "jdy_csrf_token"),
        "access_token": must_find(r'window\.jdy_access_token\s*=\s*"([^"]+)"', "jdy_access_token"),
        "access_type": must_find(r'window\.jdy_access_type\s*=\s*"([^"]+)"', "jdy_access_type"),
        "access_id": must_find(r'window\.jdy_access_id\s*=\s*"([^"]+)"', "jdy_access_id"),
        "app_id": must_find(r'appId:\s*"([^"]+)"', "appId"),
        "entry_id": must_find(r'entryId:\s*"([^"]+)"', "entryId"),
    }


def export_from_dash(dash_url: str, output_csv: Path, page_size: int = 100) -> Dict[str, Any]:
    session = requests.Session()
    resp = session.get(dash_url, timeout=30)
    resp.raise_for_status()

    auth = _extract_auth_from_html(resp.text)

    csrf_cookie = session.cookies.get("_csrf", domain=".jiandaoyun.com") or session.cookies.get("_csrf")
    if not csrf_cookie:
        raise RuntimeError("cannot find _csrf cookie from dashboard response")

    headers = {
        "Content-Type": "application/json",
        "X-CSRF-Token": auth["csrf"],
        "X-JDY-VER": "10.17.4",
        "Origin": re.match(r"^https?://[^/]+", dash_url).group(0),  # type: ignore[union-attr]
        "Referer": dash_url,
    }

    base_payload = {
        "fx_access_token": auth["access_token"],
        "fx_access_type": auth["access_type"],
        "passKey": "",
    }

    # 1) Load dashboard config to discover data table component.
    dash_config_url = f"https://www.jiandaoyun.com/_/app/{auth['app_id']}/dash/{auth['entry_id']}"
    dash_cfg_resp = session.post(dash_config_url, headers=headers, json=base_payload, timeout=30)
    dash_cfg_resp.raise_for_status()
    dash_entry = dash_cfg_resp.json().get("entry", {})
    components = dash_entry.get("components", {})
    widget_id, data_table = _pick_first_component(components, "data_table")
    form_id = str(data_table.get("form") or "")
    fields = data_table.get("fields") or []
    if not form_id:
        raise RuntimeError("data_table component has no form id")

    # 2) Infer source field names by title/type.
    title_field = _first_match_field_name(fields, ["法规名称"], field_type="text")
    category_field = _first_match_field_name(fields, ["法规分类"])
    doc_no_field = _first_match_field_name(fields, ["发布文号"], field_type="text")
    serial_field = _first_match_field_name(fields, ["编号"], field_type="sn")
    publisher_field = _first_match_field_name(fields, ["发布机关"])
    attachment_field = _first_match_field_name(fields, ["标准文件"], field_type="upload")
    status_field = _first_match_field_name(fields, ["实施状态"])

    payload = {
        "appId": auth["app_id"],
        "entryId": auth["entry_id"],
        "form": form_id,
        "widgetId": widget_id,
        **base_payload,
    }

    # 3) Count total rows.
    count_url = "https://www.jiandaoyun.com/_/data_process/data/dash/data_list/count"
    count_resp = session.post(count_url, headers=headers, json=payload, timeout=30)
    count_resp.raise_for_status()
    total = int(count_resp.json().get("count", 0))

    # 4) Page through rows.
    list_url = "https://www.jiandaoyun.com/_/data_process/data/dash/data_list/list"
    rows: List[Dict[str, Any]] = []
    for skip in range(0, total, page_size):
        page_payload = dict(payload)
        page_payload.update({"skip": skip, "limit": page_size})
        list_resp = session.post(list_url, headers=headers, json=page_payload, timeout=30)
        list_resp.raise_for_status()
        rows.extend(list_resp.json().get("data", []))

    fieldnames = [
        "legacy_id",
        "doc_no",
        "title",
        "doc_type",
        "jurisdiction",
        "publisher",
        "status",
        "category",
        "keywords",
        "summary",
        "source_url",
        "ref",
        "version_no",
        "effective_from",
        "effective_to",
        "published_at",
        "content_hash",
        "content_text",
        "attachment_url",
        "source_note",
    ]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        inserted = 0
        for i, row in enumerate(rows, start=1):
            title = str(row.get(title_field) or "").strip() if title_field else ""
            if not title:
                continue

            doc_no = ""
            if doc_no_field:
                doc_no = str(row.get(doc_no_field) or "").strip()
            if not doc_no and serial_field:
                doc_no = str(row.get(serial_field) or "").strip()

            category = str(row.get(category_field) or "").strip() if category_field else ""

            publisher = ""
            if publisher_field:
                publisher_val = row.get(publisher_field)
                if isinstance(publisher_val, list):
                    publisher = ";".join(str(x).strip() for x in publisher_val if str(x).strip())
                else:
                    publisher = str(publisher_val or "").strip()

            status = _normalize_status(row.get(status_field) if status_field else "")
            created = row.get("createTime")
            updated = row.get("updateTime") or created
            attachment_url = _first_attachment_url(row.get(attachment_field)) if attachment_field else ""

            keywords = ";".join(x for x in [category, publisher] if x)
            summary = str(row.get("label") or "").strip()[:500]

            writer.writerow(
                {
                    "legacy_id": str(900000000 + i),
                    "doc_no": _clip(doc_no, 128),
                    "title": _clip(title, 500),
                    "doc_type": "STANDARD",
                    "jurisdiction": "CN",
                    "publisher": _clip(publisher, 255),
                    "status": status,
                    "category": _clip(category, 100),
                    "keywords": keywords,
                    "summary": summary,
                    "source_url": dash_url,
                    "ref": f"v://jdy/{auth['app_id']}/{row.get('_id', '')}/v1",
                    "version_no": "1",
                    "effective_from": _to_date(created),
                    "effective_to": "",
                    "published_at": _to_date(updated),
                    "content_hash": "",
                    "content_text": "",
                    "attachment_url": attachment_url,
                    "source_note": f"imported from jdy public dash {auth['access_id']}",
                }
            )
            inserted += 1

    return {
        "dash_url": dash_url,
        "app_id": auth["app_id"],
        "entry_id": auth["entry_id"],
        "widget_id": widget_id,
        "form_id": form_id,
        "total_count": total,
        "fetched": len(rows),
        "written": inserted,
        "output": str(output_csv),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export regulations from a public JianDaoYun dashboard.")
    parser.add_argument("--dash-url", default=DEFAULT_DASH_URL, help="Public dashboard URL.")
    parser.add_argument(
        "--output",
        default="scripts/regulations_jdy_export_20260304.csv",
        help="Output CSV path.",
    )
    parser.add_argument("--page-size", type=int, default=100, help="Rows per page.")
    parser.add_argument("--json", action="store_true", help="Print result in JSON.")
    args = parser.parse_args()

    try:
        result = export_from_dash(args.dash_url, Path(args.output), page_size=args.page_size)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(
            "exported regulations:",
            f"total={result['total_count']}",
            f"fetched={result['fetched']}",
            f"written={result['written']}",
            f"output={result['output']}",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
