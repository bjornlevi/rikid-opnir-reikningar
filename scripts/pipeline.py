#!/usr/bin/env python3
"""Download Opnir reikningar export and build Parquet with DuckDB."""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import os
import sys
import zipfile
import builtins
from pathlib import Path
from typing import Iterable, Tuple

import duckdb
import requests

DEFAULT_EXPORT_URL = "https://opnirreikningar.is/rest/csvExport"


def parse_date(value: str) -> dt.date:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}', use YYYY-MM-DD") from exc


def month_chunks(start: dt.date, end: dt.date) -> Iterable[Tuple[dt.date, dt.date]]:
    if start > end:
        raise ValueError("--from must be <= --to")
    cur = start
    while cur <= end:
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        chunk_end = dt.date(cur.year, cur.month, last_day)
        if chunk_end > end:
            chunk_end = end
        yield cur, chunk_end
        cur = chunk_end + dt.timedelta(days=1)


def fmt_export_date(value: dt.date) -> str:
    return value.strftime("%d.%m.%Y")


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


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


def sniff_delimiter(sample: str) -> str:
    # Basic heuristic: choose the most frequent delimiter in the header row.
    first_line = sample.splitlines()[0] if sample else ""
    candidates = [",", ";", "\t", "|"]
    counts = {c: first_line.count(c) for c in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ","


def is_html(sample: str) -> bool:
    trimmed = sample.lstrip().lower()
    return trimmed.startswith("<!doctype") or trimmed.startswith("<html")


def _import_openpyxl_robust():
    try:
        import openpyxl
        from openpyxl.utils.datetime import from_excel
        return openpyxl, from_excel
    except Exception as first_exc:
        # openpyxl optionally imports numpy; if numpy is installed but incompatible with CPU,
        # retry while forcing numpy imports to behave as missing.
        orig_import = builtins.__import__

        def _blocked_numpy_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "numpy" or name.startswith("numpy."):
                raise ImportError("numpy blocked for openpyxl import")
            return orig_import(name, globals, locals, fromlist, level)

        builtins.__import__ = _blocked_numpy_import
        try:
            import importlib

            openpyxl = importlib.import_module("openpyxl")
            from_excel = importlib.import_module("openpyxl.utils.datetime").from_excel
            return openpyxl, from_excel
        except Exception as exc:
            raise RuntimeError(
                "openpyxl is required to read .xlsx files when DuckDB Excel extension is unavailable. "
                "Install it with: pip install openpyxl"
            ) from (exc if exc else first_exc)
        finally:
            builtins.__import__ = orig_import


def xlsx_to_csv(input_path: Path, output_path: Path) -> bool:
    openpyxl, from_excel = _import_openpyxl_robust()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.load_workbook(filename=str(input_path), read_only=False, data_only=False)
    try:
        sheet = wb.active
        import csv

        with output_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            has_data = False
            header_index = None
            date_col_index = None
            for i, row in enumerate(sheet.iter_rows(values_only=True)):
                clean = ["" if v is None else v for v in row]
                if header_index is None and any(v != "" for v in clean):
                    header_index = i
                    for idx, val in enumerate(clean):
                        if str(val).strip() == "Dags.greiðslu":
                            date_col_index = idx
                if header_index is not None and i > header_index and date_col_index is not None:
                    val = row[date_col_index] if date_col_index < len(row) else None
                    if isinstance(val, dt.datetime):
                        clean[date_col_index] = val.date().isoformat()
                    elif isinstance(val, dt.date):
                        clean[date_col_index] = val.isoformat()
                    elif isinstance(val, (int, float)):
                        try:
                            d = from_excel(val, wb.epoch)
                            clean[date_col_index] = d.date().isoformat()
                        except Exception:
                            pass
                writer.writerow(clean)
                if i > 0 and any(v != "" for v in clean):
                    has_data = True
            return has_data
    finally:
        wb.close()


def download_export(
    export_url: str,
    params: dict,
    out_dir: Path,
    chunk_label: str,
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = out_dir / f"opnirreikningar_{chunk_label}.download"

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
                final_path = out_dir / f"opnirreikningar_{chunk_label}.xlsx"
                tmp_path.replace(final_path)
                return final_path
            member = names[0]
            extracted_path = out_dir / member
            zf.extract(member, out_dir)
        tmp_path.unlink(missing_ok=True)
        return extracted_path

    final_path = out_dir / f"opnirreikningar_{chunk_label}{ext}"
    tmp_path.replace(final_path)
    return final_path


def detect_encoding(sample: bytes) -> str | None:
    if b"\x00" in sample:
        # Heuristic: UTF-16 has many null bytes.
        even = sample[0::2].count(0)
        odd = sample[1::2].count(0)
        if even > odd:
            return "utf-16-be"
        if odd > even:
            return "utf-16-le"
        return "utf-16"
    return None


def file_to_parquet(input_path: Path, output_path: Path) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sample = ""
    sample_bytes = b""
    if input_path.exists():
        try:
            sample_bytes = input_path.read_bytes()[:4096]
            sample = decode_sniff(sample_bytes)[:4096]
        except Exception:
            sample = ""
    con = duckdb.connect(database=":memory:")
    try:
        suffix = input_path.suffix.lower()
        if suffix in {".csv", ".txt", ".tsv"}:
            is_tab = suffix == ".tsv"
            if is_html(sample):
                raise RuntimeError(f"Downloaded file looks like HTML, not CSV: {input_path}")
            delim = sniff_delimiter(sample)
            try:
                sql = (
                    "COPY (SELECT * FROM read_csv_auto("
                    + sql_str(str(input_path))
                    + (", delim='\t'" if is_tab else "")
                    + ", sample_size=-1)) "
                    "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
                )
                con.execute(sql)
                count = con.execute(
                    "SELECT count(*) FROM read_csv_auto("
                    + sql_str(str(input_path))
                    + (", delim='\t'" if is_tab else "")
                    + ", sample_size=-1)"
                ).fetchone()[0]
                return count > 0
            except duckdb.InvalidInputException:
                encoding = detect_encoding(sample_bytes)
                if encoding:
                    sql = (
                        "COPY (SELECT * FROM read_csv("
                        + sql_str(str(input_path))
                        + ", delim="
                        + sql_str("\t" if is_tab else delim)
                        + ", header=true, ignore_errors=true, sample_size=-1, encoding="
                        + sql_str(encoding)
                        + ")) TO "
                        + sql_str(str(output_path))
                        + " (FORMAT PARQUET)"
                    )
                    con.execute(sql)
                    count = con.execute(
                        "SELECT count(*) FROM read_csv("
                        + sql_str(str(input_path))
                        + ", delim="
                        + sql_str("\t" if is_tab else delim)
                        + ", header=true, ignore_errors=true, sample_size=-1, encoding="
                        + sql_str(encoding)
                        + ")"
                    ).fetchone()[0]
                    return count > 0
                if sample.lstrip().startswith("PK"):
                    try:
                        con.execute("INSTALL excel;")
                        con.execute("LOAD excel;")
                        sql = (
                            "COPY (SELECT * FROM read_excel(" + sql_str(str(input_path)) + ")) "
                            "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
                        )
                        con.execute(sql)
                        count = con.execute(
                            "SELECT count(*) FROM read_excel(" + sql_str(str(input_path)) + ")"
                        ).fetchone()[0]
                        return count > 0
                    except Exception:
                        tmp_csv = output_path.with_suffix(".csv")
                        has_data = xlsx_to_csv(input_path, tmp_csv)
                        if not has_data:
                            try:
                                tmp_csv.unlink()
                            except FileNotFoundError:
                                pass
                            return False
                        sql = (
                            "COPY (SELECT * FROM read_csv_auto(" + sql_str(str(tmp_csv)) + ", sample_size=-1)) "
                            "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
                        )
                        con.execute(sql)
                        count = con.execute(
                            "SELECT count(*) FROM read_csv_auto(" + sql_str(str(tmp_csv)) + ", sample_size=-1)"
                        ).fetchone()[0]
                        try:
                            tmp_csv.unlink()
                        except FileNotFoundError:
                            pass
                        return count > 0
                # Fallback to manual delimiter and relaxed parsing.
                sql = (
                    "COPY (SELECT * FROM read_csv("
                    + sql_str(str(input_path))
                    + ", delim="
                    + sql_str(delim)
                    + ", header=true, ignore_errors=true, sample_size=-1)) "
                    "TO "
                    + sql_str(str(output_path))
                    + " (FORMAT PARQUET)"
                )
                con.execute(sql)
                count = con.execute(
                    "SELECT count(*) FROM read_csv("
                    + sql_str(str(input_path))
                    + ", delim="
                    + sql_str(delim)
                    + ", header=true, ignore_errors=true, sample_size=-1)"
                ).fetchone()[0]
                return count > 0
            return False
        elif suffix in {".xlsx", ".xls"}:
            # Always use openpyxl -> CSV -> DuckDB to normalize dates.
            tmp_csv = output_path.with_suffix(".csv")
            has_data = xlsx_to_csv(input_path, tmp_csv)
            if not has_data:
                try:
                    tmp_csv.unlink()
                except FileNotFoundError:
                    pass
                return False
            sql = (
                "COPY (SELECT * FROM read_csv_auto(" + sql_str(str(tmp_csv)) + ", sample_size=-1)) "
                "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
            )
            con.execute(sql)
            count = con.execute(
                "SELECT count(*) FROM read_csv_auto(" + sql_str(str(tmp_csv)) + ", sample_size=-1)"
            ).fetchone()[0]
            try:
                tmp_csv.unlink()
            except FileNotFoundError:
                pass
            return count > 0
        elif suffix in {".json"}:
            sql = (
                "COPY (SELECT * FROM read_json_auto(" + sql_str(str(input_path)) + ")) "
                "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
            )
            con.execute(sql)
            count = con.execute(
                "SELECT count(*) FROM read_json_auto(" + sql_str(str(input_path)) + ")"
            ).fetchone()[0]
            return count > 0
        else:
            sql = (
                "COPY (SELECT * FROM read_csv_auto(" + sql_str(str(input_path)) + ", sample_size=-1)) "
                "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
            )
            con.execute(sql)
            count = con.execute(
                "SELECT count(*) FROM read_csv_auto(" + sql_str(str(input_path)) + ", sample_size=-1)"
            ).fetchone()[0]
            return count > 0
    finally:
        con.close()
    return False


def combine_parquet(chunks_dir: Path, output_path: Path) -> None:
    con = duckdb.connect(database=":memory:")
    try:
        glob = str(chunks_dir / "*.parquet")
        sql = (
            "COPY (SELECT * FROM parquet_scan(" + sql_str(glob) + ")) "
            "TO " + sql_str(str(output_path)) + " (FORMAT PARQUET)"
        )
        con.execute(sql)
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="date_from", type=parse_date, required=True)
    parser.add_argument("--to", dest="date_to", type=parse_date, required=True)
    parser.add_argument("--org-id", default="")
    parser.add_argument("--vendor-id", default="")
    parser.add_argument("--type-id", default="")
    parser.add_argument("--export-url", default=os.environ.get("OPNIR_EXPORT_URL", DEFAULT_EXPORT_URL))
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--parquet-dir", default="data/parquet")
    parser.add_argument(
        "--fallback-xlsx",
        default=os.environ.get("OPNIR_FALLBACK_XLSX", "data/raw/export.xlsx"),
        help="Local export.xlsx to use if download fails.",
    )
    parser.add_argument("--force-download", action="store_true", help="Always download even if raw exists.")

    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    parquet_dir = Path(args.parquet_dir)
    chunks_dir = parquet_dir / "chunks"
    fallback_xlsx = Path(args.fallback_xlsx)

    chunk_paths = []
    for start, end in month_chunks(args.date_from, args.date_to):
        chunk_label = f"{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        params = {
            "org_id": args.org_id,
            "vendor_id": args.vendor_id,
            "type_id": args.type_id,
            "timabil_fra": fmt_export_date(start),
            "timabil_til": fmt_export_date(end),
        }
        existing_raw = None
        if not args.force_download:
            for ext in (".xlsx", ".csv", ".tsv", ".json"):
                candidate = raw_dir / f"opnirreikningar_{chunk_label}{ext}"
                if candidate.exists():
                    existing_raw = candidate
                    break

        if existing_raw:
            print(f"Using existing raw file: {existing_raw}")
            downloaded = existing_raw
        else:
            print(f"Downloading {chunk_label}...")
            try:
                downloaded = download_export(args.export_url, params, raw_dir, chunk_label)
                print(f"  Saved: {downloaded}")
            except requests.RequestException as exc:
                if fallback_xlsx.exists() and not chunk_paths:
                    print(f"  Download failed ({exc}). Using fallback file: {fallback_xlsx}")
                    downloaded = fallback_xlsx
                else:
                    raise

        out_parquet = chunks_dir / f"opnirreikningar_{chunk_label}.parquet"
        print(f"  Converting to Parquet: {out_parquet}")
        has_rows = file_to_parquet(downloaded, out_parquet)
        if has_rows:
            chunk_paths.append(out_parquet)
        else:
            print("  No data rows found, skipping chunk.")
            try:
                out_parquet.unlink()
            except FileNotFoundError:
                pass
            if fallback_xlsx.exists() and downloaded != fallback_xlsx and not chunk_paths:
                fallback_parquet = chunks_dir / "opnirreikningar_fallback.parquet"
                print(f"  Using fallback file instead: {fallback_xlsx}")
                has_fallback_rows = file_to_parquet(fallback_xlsx, fallback_parquet)
                if has_fallback_rows:
                    chunk_paths.append(fallback_parquet)
                    break
                print("  Fallback file has no data rows.")
                return 1

        if downloaded == fallback_xlsx:
            break

    if not chunk_paths:
        print("No chunks produced.")
        return 1

    final_parquet = parquet_dir / "opnirreikningar.parquet"
    print(f"Combining chunks into {final_parquet}")
    combine_parquet(chunks_dir, final_parquet)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
