# Rikid – Opnir reikningar pipeline + Flask viewer

This project downloads the **"Flytja út"** export from https://www.opnirreikningar.is/, converts it to Parquet with DuckDB, and serves a simple Flask UI to browse the data.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Build Parquet (example range)
make pipeline FROM=2021-08-01 TO=2021-08-31

# Build Parquet from earliest available date
make pipeline TO=2026-01-28

# Run the app
make web
```

Open: http://127.0.0.1:5000/

## Pipeline

The export endpoint (used by the **"Flytja út"** button) is a CSV export:

```
https://opnirreikningar.is/rest/csvExport
```

It accepts query parameters:
- `org_id`
- `vendor_id`
- `type_id`
- `timabil_fra` (from date in `DD.MM.YYYY`)
- `timabil_til` (to date in `DD.MM.YYYY`)

The pipeline splits the requested date range into monthly chunks (to avoid size limits), downloads each CSV, converts each to Parquet, and writes a combined `data/parquet/opnirreikningar.parquet`.
If the export returns an `.xlsx` file (the same as clicking **\"Flytja út\"** in the UI), the pipeline detects it and uses DuckDB's Excel reader. If that extension isn’t available locally, it falls back to `openpyxl` to convert to CSV first.

### Example

```bash
make pipeline \
  FROM=2021-08-01 \
  TO=2021-10-31 \
  ORG_ID=1010
```

Force re-downloads (ignore existing raw files):

```bash
make pipeline FROM=2021-08-01 TO=2021-08-31 FORCE_DOWNLOAD=1
```

## Find the earliest available date

If you want to download **all** available data but don’t know the first date, use the probe script:

```bash
make earliest
```

This will search from 2017-01-01 through today and print the earliest month with data (fast).
You can constrain the search window:

```bash
make earliest SEARCH_FROM=2005-01-01 SEARCH_TO=2010-12-31
```

If you need the exact earliest day (slower), use:

```bash
make earliest EXACT=1
```

### Environment variables

- `OPNIR_EXPORT_URL` – override the export URL (default: `https://opnirreikningar.is/rest/csvExport`)
- `OPNIR_PARQUET` – path to parquet file for the Flask app (default: `data/parquet/opnirreikningar.parquet`)
- `OPNIR_FALLBACK_XLSX` – local `export.xlsx` to use if download fails (default: `data/raw/export.xlsx`)
- `PAGE_SIZE` – rows per page in the UI (default: `50`)

## Folder structure

```
app/                 Flask app
scripts/             Pipeline scripts
data/raw/            Downloaded CSVs
data/parquet/        Combined parquet
```
