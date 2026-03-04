#!/usr/bin/env python3
"""Validate naming conventions in scripts/ directory."""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "migrations"

ALLOWED_EXTENSIONS = {
    ".sql",
    ".py",
    ".sh",
    ".ps1",
    ".js",
    ".md",
    ".csv",
    ".json",
}

ROOT_SQL_PREFIX_RE = re.compile(
    r"^(add|create|backfill|cleanup|delete|fix|verify|query|apply|bootstrap|migrate|"
    r"activate|auto|implement|ratify|restructure)_[a-z0-9][a-z0-9_]*\.sql$"
)
DATE_SQL_RE = re.compile(r"^\d{8}_[a-z0-9][a-z0-9_]*\.sql$")
GENERIC_FILENAME_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")


def iter_files(base: Path):
    for p in sorted(base.rglob("*")):
        if p.is_dir():
            continue
        rel_parts = p.relative_to(base).parts
        if "__pycache__" in rel_parts:
            continue
        yield p


def check() -> int:
    errors: list[str] = []
    warnings: list[str] = []

    rebuild_files: list[Path] = []

    for file_path in iter_files(ROOT):
        rel = file_path.relative_to(ROOT)
        name = file_path.name
        ext = file_path.suffix.lower()

        if ext not in ALLOWED_EXTENSIONS:
            errors.append(f"unsupported extension: {rel}")

        if name != "README.md" and not GENERIC_FILENAME_RE.match(name):
            errors.append(f"invalid filename chars/case: {rel}")

        in_migrations = MIGRATIONS in file_path.parents

        if ext == ".sql" and in_migrations:
            if not DATE_SQL_RE.match(name):
                warnings.append(
                    f"migration sql must be date-prefixed YYYYMMDD_action.sql: {rel}"
                )

        if ext == ".sql" and not in_migrations:
            if not (ROOT_SQL_PREFIX_RE.match(name) or DATE_SQL_RE.match(name)):
                warnings.append(
                    f"root sql not in preferred naming style (verb_prefix or date_prefix): {rel}"
                )

            if name.endswith("_rebuild.sql"):
                rebuild_files.append(file_path)
                warnings.append(
                    f"legacy suffix '_rebuild' detected (prefer '_compat' for new files): {rel}"
                )

    for rebuild in rebuild_files:
        base_name = rebuild.name.replace("_rebuild.sql", ".sql")
        base_file = rebuild.parent / base_name
        if not base_file.exists():
            warnings.append(
                f"rebuild variant without base pair: {rebuild.relative_to(ROOT)}"
            )

    if warnings:
        print("[WARN]")
        for w in warnings:
            print(f"- {w}")

    if errors:
        print("[ERROR]")
        for e in errors:
            print(f"- {e}")
        return 1

    print("scripts naming check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(check())

