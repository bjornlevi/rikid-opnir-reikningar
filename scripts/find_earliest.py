#!/usr/bin/env python3
"""Find the earliest date with data in Opnir reikningar export."""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

DEFAULT_EXPORT_URL = "https://opnirreikningar.is/rest/csvExport"


def parse_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', use YYYY-MM-DD") from exc


def fmt_export_date(value: dt.date) -> str:
    return value.strftime("%d.%m.%Y")


def month_end(value: dt.date) -> dt.date:
    last_day = calendar.monthrange(value.year, value.month)[1]
    return dt.date(value.year, value.month, last_day)


def decode_sniff(sniff: bytes) -> str:
    for enc in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            return sniff.decode(enc)
        except Exception:
            continue
    return ""


def guess_extension(content_type: str, sniff: bytes) -> str:
    sniff = sniff.lstrip()
    if sniff.startswith(b"PK"):
        return ".zip"
    if sniff.startswith(b"{") or sniff.startswith(b"["):
        return ".json"
    text = decode_sniff(sniff)
    if "\t" in text and "\n" in text:
        return ".tsv"
    ct = (content_type or "").lower()
    if "zip" in ct:
        return ".zip"
    if "json" in ct:
        return ".json"
    if "csv" in ct or "excel" in ct or "text/plain" in ct:
        return ".csv"
    return ".csv"


def download_range(export_url: str, params: dict, out_dir: Path, label: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = out_dir / f"probe_{label}.download"

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/zip, text/csv, text/tab-separated-values, */*",
        "Referer": "https://www.opnirreikningar.is/",
    }
    with requests.get(export_url, params=params, headers=headers, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with tmp_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        content_type = resp.headers.get("content-type", "")

    sniff = tmp_path.open("rb").read(2048)
    ext = guess_extension(content_type, sniff)

    if ext == ".zip":
        with zipfile.ZipFile(tmp_path) as zf:
            names = zf.namelist()
            if not names:
                raise RuntimeError("Zip file is empty")
            if any(name.lower() == "xl/workbook.xml" for name in names):
                final_path = out_dir / f"probe_{label}.xlsx"
                tmp_path.replace(final_path)
                return final_path
            member = names[0]
            extracted_path = out_dir / member
            zf.extract(member, out_dir)
        tmp_path.unlink(missing_ok=True)
        return extracted_path

    final_path = out_dir / f"probe_{label}{ext}"
    tmp_path.replace(final_path)
    return final_path


def has_data(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt", ".tsv"}:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            header = f.readline()
            if not header:
                return False
            # Read a second non-empty line (data row)
            while True:
                line = f.readline()
                if not line:
                    return False
                if line.strip():
                    return True
    if suffix == ".xlsx":
        # Lightweight check without openpyxl/numpy: count non-empty rows in worksheet XML.
        try:
            with zipfile.ZipFile(path) as zf:
                sheet_files = [
                    name
                    for name in zf.namelist()
                    if name.startswith("xl/worksheets/") and name.endswith(".xml")
                ]
                for sheet_name in sheet_files:
                    non_empty_rows = 0
                    with zf.open(sheet_name) as fh:
                        for event, elem in ET.iterparse(fh, events=("end",)):
                            if not elem.tag.endswith("row"):
                                continue
                            has_any = False
                            for child in elem.iter():
                                tag = child.tag
                                if tag.endswith("v") or tag.endswith("t") or tag.endswith("f"):
                                    if child.text and child.text.strip():
                                        has_any = True
                                        break
                            if has_any:
                                non_empty_rows += 1
                                if non_empty_rows >= 2:
                                    return True
                            elem.clear()
            return False
        except zipfile.BadZipFile:
            return False
    if suffix == ".xls":
        # Legacy format: fallback to non-empty file check.
        return path.stat().st_size > 0
    if suffix == ".json":
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            data = f.read(4096).lstrip()
            if data.startswith("["):
                return not data.startswith("[]")
            if data.startswith("{"):
                return True
        return False
    # Fallback: treat unknown as data-present if non-empty
    return path.stat().st_size > 0


def probe_range(export_url: str, params: dict, tmp_dir: Path, label: str) -> bool:
    for ext in (".xlsx", ".csv", ".tsv", ".json"):
        existing = tmp_dir / f"opnirreikningar_{label}{ext}"
        if existing.exists():
            return has_data(existing)

    path = download_range(export_url, params, tmp_dir, label)
    try:
        return has_data(path)
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def find_earliest(
    export_url: str,
    start: dt.date,
    end: dt.date,
    params: dict,
    tmp_dir: Path,
    verbose: bool,
    exact: bool,
) -> dt.date | None:
    # Step 1: scan by month to find first month with data
    cur = start
    first_month = None
    while cur <= end:
        m_end = month_end(cur)
        if m_end > end:
            m_end = end
        label = f"{cur.strftime('%Y%m%d')}_{m_end.strftime('%Y%m%d')}"
        params.update({
            "timabil_fra": fmt_export_date(cur),
            "timabil_til": fmt_export_date(m_end),
        })
        if verbose:
            print(f"Probing month {label}...")
        if probe_range(export_url, params, tmp_dir, label):
            first_month = (cur, m_end)
            break
        cur = m_end + dt.timedelta(days=1)

    if not first_month:
        return None

    if not exact:
        return first_month[0]

    # Step 2: binary search within the month for earliest day with data
    low, high = first_month
    while low < high:
        mid = low + dt.timedelta(days=(high - low).days // 2)
        label = f"{low.strftime('%Y%m%d')}_{mid.strftime('%Y%m%d')}"
        params.update({
            "timabil_fra": fmt_export_date(low),
            "timabil_til": fmt_export_date(mid),
        })
        if verbose:
            print(f"Probing range {label}...")
        if probe_range(export_url, params, tmp_dir, label):
            high = mid
        else:
            low = mid + dt.timedelta(days=1)
    return low


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--search-from", type=parse_date, default=dt.date(2017, 1, 1))
    parser.add_argument("--search-to", type=parse_date, default=dt.date.today())
    parser.add_argument("--org-id", default="")
    parser.add_argument("--vendor-id", default="")
    parser.add_argument("--type-id", default="")
    parser.add_argument("--export-url", default=os.environ.get("OPNIR_EXPORT_URL", DEFAULT_EXPORT_URL))
    parser.add_argument("--tmp-dir", default="data/raw")
    parser.add_argument("--print-only", action="store_true", help="Print only the date value.")
    parser.add_argument("--exact", action="store_true", help="Find the exact earliest day (slower).")

    args = parser.parse_args()

    params = {
        "org_id": args.org_id,
        "vendor_id": args.vendor_id,
        "type_id": args.type_id,
    }

    earliest = find_earliest(
        args.export_url,
        args.search_from,
        args.search_to,
        params,
        Path(args.tmp_dir),
        verbose=not args.print_only,
        exact=args.exact,
    )

    if not earliest:
        if not args.print_only:
            print("No data found in the given range.")
        return 1

    if args.print_only:
        print(earliest.isoformat())
    else:
        print(f"Earliest date with data: {earliest.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
