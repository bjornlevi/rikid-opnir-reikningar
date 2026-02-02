from __future__ import annotations

import math
import os
from pathlib import Path

import duckdb
from flask import Flask, render_template, request, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET = BASE_DIR / "data" / "parquet" / "opnirreikningar.parquet"
OPNIR_PREFIX = os.getenv("OPNIR_PREFIX", "").rstrip("/")

COLUMN_NAMES = {
    "Kaupandi": "Kaupandi",
    "Dags.greiðslu": "Dags.greiðslu",
    "Birgi": "Birgi",
    "Tegund": "Tegund",
    "Númer reiknings": "Númer reiknings",
    "Upphæð línu": "Upphæð línu",
}

CLICKABLE_COLUMNS = {
    "Kaupandi": "buyer",
    "Birgi": "vendor",
    "Tegund": "type",
}


def _format_number(value) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:,.0f}".replace(",", ".")


def _build_where(year: str, buyer: str, vendor: str, entry_type: str, exclude: str | None = None) -> tuple[str, list]:
    clauses = []
    params: list = []

    if year and year != "all" and exclude != "year":
        try:
            clauses.append('YEAR("Dags.greiðslu") = ?')
            params.append(int(year))
        except ValueError:
            pass
    if buyer and buyer != "all" and exclude != "buyer":
        clauses.append('"Kaupandi" = ?')
        params.append(buyer)
    if vendor and vendor != "all" and exclude != "vendor":
        clauses.append('"Birgi" = ?')
        params.append(vendor)
    if entry_type and entry_type != "all" and exclude != "type":
        clauses.append('"Tegund" = ?')
        params.append(entry_type)

    where_sql = " AND ".join(clauses)
    if where_sql:
        where_sql = "WHERE " + where_sql
    return where_sql, params


def _toggle_value(current: str, value: str) -> str:
    return "all" if value == current else value


def _build_links(items, current: str, key: str, base_params: dict, label_fn=None):
    links = []
    for item in items:
        value = str(item)
        params = dict(base_params)
        params[key] = _toggle_value(current, value)
        params["page"] = 1
        label = label_fn(item) if label_fn else value
        links.append({
            "label": label,
            "value": value,
            "url": url_for("index", **params),
            "active": value == current,
        })
    return links


def _cell_url(col_name: str, value, year: str, buyer: str, vendor: str, entry_type: str) -> str:
    params = {
        "year": year,
        "buyer": buyer,
        "vendor": vendor,
        "type": entry_type,
        "page": 1,
    }
    filter_key = CLICKABLE_COLUMNS.get(col_name)
    if filter_key:
        current = params.get(filter_key, "all")
        params[filter_key] = _toggle_value(str(current), str(value))
    return url_for("index", **params)


def create_app() -> Flask:
    app = Flask(__name__)

    if OPNIR_PREFIX:
        app.config["APPLICATION_ROOT"] = OPNIR_PREFIX
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

        class PrefixMiddleware:
            def __init__(self, app, prefix: str):
                self.app = app
                self.prefix = prefix

            def __call__(self, environ, start_response):
                script_name = self.prefix
                path_info = environ.get("PATH_INFO", "")
                if path_info.startswith(script_name):
                    environ["SCRIPT_NAME"] = script_name
                    environ["PATH_INFO"] = path_info[len(script_name):] or "/"
                return self.app(environ, start_response)

        app.wsgi_app = PrefixMiddleware(app.wsgi_app, OPNIR_PREFIX)

    @app.route("/")
    def index():
        parquet_path = Path(os.environ.get("OPNIR_PARQUET", DEFAULT_PARQUET)).resolve()
        page_size = max(1, int(os.environ.get("PAGE_SIZE", "50")))
        page = max(1, int(request.args.get("page", "1")))
        offset = (page - 1) * page_size

        year = request.args.get("year") or "all"
        buyer = request.args.get("buyer") or "all"
        vendor = request.args.get("vendor") or "all"
        entry_type = request.args.get("type") or "all"

        if not parquet_path.exists():
            return render_template(
                "index.html",
                missing=True,
                parquet_path=parquet_path,
                rows=[],
                columns=[],
                page=page,
                pages=0,
                total=0,
                page_size=page_size,
                year=year,
                buyer=buyer,
                vendor=vendor,
                entry_type=entry_type,
                year_links=[],
                buyer_links=[],
                vendor_links=[],
                type_links=[],
                clickable_columns=CLICKABLE_COLUMNS,
            )

        con = duckdb.connect(database=":memory:")
        try:
            parquet_sql = str(parquet_path).replace("'", "''")
            con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{parquet_sql}')")

            where_sql, where_params = _build_where(year, buyer, vendor, entry_type)

            total = con.execute(
                f"SELECT count(*) FROM data {where_sql}",
                where_params,
            ).fetchone()[0]

            amount_expr = 'TRY_CAST("Upphæð línu" AS DOUBLE)'
            totals_row = con.execute(
                f"""
                SELECT
                    SUM({amount_expr}) AS sum_total,
                    SUM(CASE WHEN {amount_expr} > 0 THEN {amount_expr} END) AS sum_pos,
                    SUM(CASE WHEN {amount_expr} < 0 THEN {amount_expr} END) AS sum_neg
                FROM data {where_sql}
                """,
                where_params,
            ).fetchone()
            sum_total = float(totals_row[0] or 0)
            sum_pos = float(totals_row[1] or 0)
            sum_neg = float(totals_row[2] or 0)
            sum_total_display = _format_number(sum_total)
            sum_pos_display = _format_number(sum_pos)
            sum_neg_display = _format_number(abs(sum_neg))

            raw_rows = con.execute(
                f'SELECT * FROM data {where_sql} ORDER BY "Dags.greiðslu" LIMIT ? OFFSET ?',
                [*where_params, page_size, offset],
            ).fetchall()
            columns = [col[0] for col in con.description]
            rows = []
            for row in raw_rows:
                item = dict(zip(columns, row))
                if "Upphæð línu" in item and item["Upphæð línu"] is not None:
                    item["Upphæð línu"] = _format_number(item["Upphæð línu"])
                rows.append(item)

            years = [
                row[0]
                for row in con.execute(
                    'SELECT DISTINCT YEAR("Dags.greiðslu") AS y FROM data WHERE "Dags.greiðslu" IS NOT NULL ORDER BY y'
                ).fetchall()
                if row[0] is not None
            ]

            buyer_where, buyer_params = _build_where(year, buyer, vendor, entry_type, exclude="buyer")
            buyer_rows = con.execute(
                f'SELECT "Kaupandi", COUNT(*) FROM data {buyer_where} '
                'GROUP BY "Kaupandi" ORDER BY COUNT(*) DESC LIMIT 50',
                buyer_params,
            ).fetchall()
            vendor_where, vendor_params = _build_where(year, buyer, vendor, entry_type, exclude="vendor")
            vendor_rows = con.execute(
                f'SELECT "Birgi", COUNT(*) FROM data {vendor_where} '
                'GROUP BY "Birgi" ORDER BY COUNT(*) DESC LIMIT 50',
                vendor_params,
            ).fetchall()
            type_where, type_params = _build_where(year, buyer, vendor, entry_type, exclude="type")
            type_rows = con.execute(
                f'SELECT "Tegund", COUNT(*) FROM data {type_where} '
                'GROUP BY "Tegund" ORDER BY COUNT(*) DESC LIMIT 50',
                type_params,
            ).fetchall()
        finally:
            con.close()

        pages = max(1, math.ceil(total / page_size)) if total else 0
        page = min(page, pages) if pages else 1

        base_params = {
            "year": year,
            "buyer": buyer,
            "vendor": vendor,
            "type": entry_type,
            "page": page,
        }
        year_links = _build_links(
            ["all"] + [str(y) for y in years],
            year,
            "year",
            base_params,
            label_fn=lambda x: "All" if x == "all" else x,
        )
        buyer_links = [{
            "label": "All",
            "value": "all",
            "url": url_for("index", **{**base_params, "buyer": "all", "page": 1}),
            "active": buyer == "all",
        }]
        for value, count in buyer_rows:
            if value is None:
                continue
            buyer_links.append({
                "label": f"{value} ({_format_number(count)})",
                "value": value,
                "url": url_for("index", **{**base_params, "buyer": _toggle_value(buyer, value), "page": 1}),
                "active": value == buyer,
            })
        vendor_links = [{
            "label": "All",
            "value": "all",
            "url": url_for("index", **{**base_params, "vendor": "all", "page": 1}),
            "active": vendor == "all",
        }]
        for value, count in vendor_rows:
            if value is None:
                continue
            vendor_links.append({
                "label": f"{value} ({_format_number(count)})",
                "value": value,
                "url": url_for("index", **{**base_params, "vendor": _toggle_value(vendor, value), "page": 1}),
                "active": value == vendor,
            })
        type_links = [{
            "label": "All",
            "value": "all",
            "url": url_for("index", **{**base_params, "type": "all", "page": 1}),
            "active": entry_type == "all",
        }]
        for value, count in type_rows:
            if value is None:
                continue
            type_links.append({
                "label": f"{value} ({_format_number(count)})",
                "value": value,
                "url": url_for("index", **{**base_params, "type": _toggle_value(entry_type, value), "page": 1}),
                "active": value == entry_type,
            })

        return render_template(
            "index.html",
            missing=False,
            parquet_path=parquet_path,
            rows=rows,
            columns=columns,
            page=page,
            pages=pages,
            total=total,
            sum_total=sum_total,
            sum_pos=sum_pos,
            sum_neg=sum_neg,
            sum_total_display=sum_total_display,
            sum_pos_display=sum_pos_display,
            sum_neg_display=sum_neg_display,
            page_size=page_size,
            year=year,
            buyer=buyer,
            vendor=vendor,
            entry_type=entry_type,
            year_links=year_links,
            buyer_links=buyer_links,
            vendor_links=vendor_links,
            type_links=type_links,
            clickable_columns=CLICKABLE_COLUMNS,
            cell_url=_cell_url,
        )

    return app


app = create_app()
