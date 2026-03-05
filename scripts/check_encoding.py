#!/usr/bin/env python3
"""Repository encoding guard.

Checks tracked text files for:
1) UTF-8 BOM
2) Unicode replacement character U+FFFD
3) Likely mojibake lines (UTF-8 Chinese decoded as GBK-like text)
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys

TEXT_EXT = {
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".go",
    ".py",
    ".sql",
    ".md",
    ".yaml",
    ".yml",
    ".json",
    ".css",
    ".html",
    ".sh",
    ".ps1",
    ".txt",
    ".csv",
    ".xml",
}

CJK_RE = re.compile(r"[\u4e00-\u9fff]")
COMMON_CN = set("的一是不了在人有和为中与到上个这们来时大地要就出会可也你对生能而子那得于着下自之年过发后作里用道行所然家种事成方多经么去法学如都同现当没动面起看定天分还进好小部其些主样理心她本前开但因只从想实")


def git_tracked_files() -> list[pathlib.Path]:
    out = subprocess.check_output(["git", "ls-files"], text=True, encoding="utf-8")
    files: list[pathlib.Path] = []
    for line in out.splitlines():
        p = pathlib.Path(line.strip())
        if not p.exists():
            continue
        if p.suffix.lower() in TEXT_EXT:
            files.append(p)
    return files


def count_common_cn(s: str) -> int:
    return sum(ch in COMMON_CN for ch in s)


def likely_mojibake_line(line: str) -> bool:
    if not CJK_RE.search(line):
        return False

    high_range = sum(0x9300 <= ord(ch) <= 0x9FFF for ch in line)
    if high_range < 2:
        return False

    try:
        rev = line.encode("gbk").decode("utf-8")
    except Exception:
        return False

    if rev == line:
        return False

    s1 = count_common_cn(line)
    s2 = count_common_cn(rev)
    punct = "，。：；（）、《》、！？"
    p1 = sum(ch in punct for ch in line)
    p2 = sum(ch in punct for ch in rev)
    return s2 >= s1 + 2 and p2 >= p1 + 1


def main() -> int:
    bom_hits: list[str] = []
    replacement_hits: list[str] = []
    mojibake_hits: list[str] = []

    for p in git_tracked_files():
        data = p.read_bytes()
        if data.startswith(b"\xef\xbb\xbf"):
            bom_hits.append(str(p))

        text = data.decode("utf-8", errors="replace")
        if "\ufffd" in text:
            replacement_hits.append(str(p))

        for idx, line in enumerate(text.splitlines(), 1):
            if likely_mojibake_line(line):
                mojibake_hits.append(f"{p}:{idx}: {line.strip()}")

    if bom_hits:
        print("UTF-8 BOM files:")
        for x in bom_hits:
            print(f"  - {x}")
    if replacement_hits:
        print("Files containing U+FFFD:")
        for x in replacement_hits:
            print(f"  - {x}")
    if mojibake_hits:
        print("Likely mojibake lines:")
        for x in mojibake_hits[:200]:
            print(f"  - {x}")

    failed = bool(bom_hits or replacement_hits or mojibake_hits)
    if failed:
        print("Encoding check: FAILED")
        return 1

    print("Encoding check: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
