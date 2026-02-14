from __future__ import annotations

import csv
import io
import json
import math
import os
from pathlib import Path

import duckdb
from flask import Flask, Response, render_template, request, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET = BASE_DIR / "data" / "parquet" / "opnirreikningar.parquet"
DEFAULT_ANOMALIES_PARQUET = Path(
    os.environ.get("OPNIR_ANOMALIES_PARQUET", str(BASE_DIR / "data" / "parquet" / "anomalies_flagged.parquet"))
)
DEFAULT_ANOMALIES_ALL_PARQUET = Path(
    os.environ.get("OPNIR_ANOMALIES_ALL_PARQUET", str(BASE_DIR / "data" / "parquet" / "anomalies_yearly_all.parquet"))
)
OPNIR_PREFIX = os.getenv("OPNIR_PREFIX", "").rstrip("/")

CLICKABLE_COLUMNS = {
    "Kaupandi": "buyer",
    "Birgi": "vendor",
    "Tegund": "type",
}

ANALYSIS_PARENT_COLUMNS = {
    "buyer": "Kaupandi",
    "vendor": "Birgi",
    "type": "Tegund",
}
ANALYSIS_CHILD_COLUMNS = [
    "Kaupandi",
    "Birgi",
    "Tegund",
    "Númer reiknings",
]
ANOMALY_PARENT_COLUMNS = [
    "Kaupandi",
    "Birgi",
    "Tegund",
]


def _format_number(value) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:,.0f}".replace(",", ".")


def _safe_path(path: Path) -> str:
    return str(path).replace("'", "''")


def _open_data_connection(parquet_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet('{_safe_path(parquet_path)}')")
    return con


def _open_anomalies_connection(parquet_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE OR REPLACE VIEW anomalies AS SELECT * FROM read_parquet('{_safe_path(parquet_path)}')")
    return con


def _open_anomalies_all_connection(parquet_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE OR REPLACE VIEW anomalies_all AS SELECT * FROM read_parquet('{_safe_path(parquet_path)}')")
    return con


def _toggle_value(current: str, value: str) -> str:
    return "all" if value == current else value


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


def _build_index_cell_url(col_name: str, value, year: str, buyer: str, vendor: str, entry_type: str) -> str:
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


def _analysis_url(base: dict, **updates) -> str:
    params = dict(base)
    params.update(updates)
    return url_for("analysis", **params)


def _anomalies_url(base: dict, **updates) -> str:
    params = dict(base)
    params.update(updates)
    return url_for("anomalies", **params)


def _reports_url(base: dict, **updates) -> str:
    params = dict(base or {})
    params.update(updates)
    return url_for("reports", **params)


def _analysis_scope_from_request(con: duckdb.DuckDBPyConnection):
    parent_type = request.args.get("parent_type", "buyer")
    if parent_type not in ANALYSIS_PARENT_COLUMNS:
        parent_type = "buyer"
    parent_col = ANALYSIS_PARENT_COLUMNS[parent_type]

    child_keys = [c for c in ANALYSIS_CHILD_COLUMNS if c != parent_col]
    child_key = request.args.get("child_key") or (child_keys[0] if child_keys else "")
    if child_key not in child_keys:
        child_key = child_keys[0] if child_keys else ""

    parent_options = [
        r[0]
        for r in con.execute(
            f'SELECT DISTINCT "{parent_col}" FROM data WHERE "{parent_col}" IS NOT NULL ORDER BY "{parent_col}" LIMIT 1500'
        ).fetchall()
    ]
    parent_value = request.args.get("parent_value") or (parent_options[0] if parent_options else "")
    if parent_value and parent_value not in parent_options:
        parent_value = parent_options[0] if parent_options else ""

    child_value = request.args.get("child_value", "")
    analysis_year = request.args.get("analysis_year", "all")
    return parent_type, parent_col, child_key, parent_value, child_value, child_keys, parent_options, analysis_year


def _write_csv_response(rows: list[dict], filename: str) -> Response:
    if not rows:
        output = ""
    else:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        output = buf.getvalue()
    return Response(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


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

        con = _open_data_connection(parquet_path)
        try:
            where_sql, where_params = _build_where(year, buyer, vendor, entry_type)

            total = con.execute(f"SELECT count(*) FROM data {where_sql}", where_params).fetchone()[0]
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

            raw_rows = con.execute(
                f'SELECT * FROM data {where_sql} ORDER BY "Dags.greiðslu" DESC LIMIT ? OFFSET ?',
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
                f'SELECT "Kaupandi", COUNT(*) FROM data {buyer_where} GROUP BY "Kaupandi" ORDER BY COUNT(*) DESC LIMIT 50',
                buyer_params,
            ).fetchall()
            vendor_where, vendor_params = _build_where(year, buyer, vendor, entry_type, exclude="vendor")
            vendor_rows = con.execute(
                f'SELECT "Birgi", COUNT(*) FROM data {vendor_where} GROUP BY "Birgi" ORDER BY COUNT(*) DESC LIMIT 50',
                vendor_params,
            ).fetchall()
            type_where, type_params = _build_where(year, buyer, vendor, entry_type, exclude="type")
            type_rows = con.execute(
                f'SELECT "Tegund", COUNT(*) FROM data {type_where} GROUP BY "Tegund" ORDER BY COUNT(*) DESC LIMIT 50',
                type_params,
            ).fetchall()
        finally:
            con.close()

        pages = max(1, math.ceil(total / page_size)) if total else 0
        page = min(page, pages) if pages else 1

        base_params = {"year": year, "buyer": buyer, "vendor": vendor, "type": entry_type, "page": page}
        year_links = []
        for y in ["all"] + [str(v) for v in years]:
            year_links.append({
                "label": "All" if y == "all" else y,
                "value": y,
                "url": url_for("index", **{**base_params, "year": _toggle_value(year, y), "page": 1}),
                "active": y == year,
            })

        buyer_links = [{"label": "All", "value": "all", "url": url_for("index", **{**base_params, "buyer": "all", "page": 1}), "active": buyer == "all"}]
        for value, count in buyer_rows:
            if value is None:
                continue
            buyer_links.append({
                "label": f"{value} ({_format_number(count)})",
                "value": value,
                "url": url_for("index", **{**base_params, "buyer": _toggle_value(buyer, value), "page": 1}),
                "active": value == buyer,
            })

        vendor_links = [{"label": "All", "value": "all", "url": url_for("index", **{**base_params, "vendor": "all", "page": 1}), "active": vendor == "all"}]
        for value, count in vendor_rows:
            if value is None:
                continue
            vendor_links.append({
                "label": f"{value} ({_format_number(count)})",
                "value": value,
                "url": url_for("index", **{**base_params, "vendor": _toggle_value(vendor, value), "page": 1}),
                "active": value == vendor,
            })

        type_links = [{"label": "All", "value": "all", "url": url_for("index", **{**base_params, "type": "all", "page": 1}), "active": entry_type == "all"}]
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
            sum_total_display=_format_number(sum_total),
            sum_pos_display=_format_number(sum_pos),
            sum_neg_display=_format_number(abs(sum_neg)),
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
            cell_url=_build_index_cell_url,
        )

    @app.route("/analysis")
    def analysis():
        parquet_path = Path(os.environ.get("OPNIR_PARQUET", DEFAULT_PARQUET)).resolve()
        if not parquet_path.exists():
            return render_template("analysis.html", data_loaded=False, error=f"No data found at {parquet_path}.")

        con = _open_data_connection(parquet_path)
        try:
            (
                parent_type,
                parent_col,
                child_key,
                parent_value,
                child_value,
                child_keys,
                parent_options,
                analysis_year,
            ) = _analysis_scope_from_request(con)

            try:
                page = max(1, int(request.args.get("page", "1")))
            except ValueError:
                page = 1
            try:
                page_size = int(request.args.get("page_size", "100"))
            except ValueError:
                page_size = 100
            page_size = max(25, min(500, page_size))

            base_params = {
                "parent_type": parent_type,
                "parent_value": parent_value,
                "child_key": child_key,
                "child_value": child_value,
                "analysis_year": analysis_year,
                "page": page,
                "page_size": page_size,
            }

            if not parent_value or not child_key:
                return render_template(
                    "analysis.html",
                    data_loaded=True,
                    parent_type=parent_type,
                    parent_col=parent_col,
                    child_key=child_key,
                    parent_options=parent_options,
                    child_keys=child_keys,
                    parent_value=parent_value,
                    child_value=child_value,
                    analysis_year=analysis_year,
                    breakdown_rows=[],
                    record_rows=[],
                    record_columns=[],
                    yearly_labels=[],
                    yearly_values=[],
                    year_links=[],
                    page=page,
                    page_size=page_size,
                    total_records=0,
                    total_pages=1,
                    build_analysis_url=_analysis_url,
                    base_params=base_params,
                )

            amount_expr = 'TRY_CAST("Upphæð línu" AS DOUBLE)'
            scope_params = [parent_value]
            scope_where = f'"{parent_col}" = ?'
            if child_value:
                scope_where += f' AND "{child_key}" = ?'
                scope_params.append(child_value)

            graph_rows = con.execute(
                f"""
                SELECT YEAR("Dags.greiðslu") AS y, SUM({amount_expr}) AS amount_sum
                FROM data
                WHERE {scope_where} AND "Dags.greiðslu" IS NOT NULL
                GROUP BY y
                ORDER BY y
                """,
                scope_params,
            ).fetchall()
            yearly_labels = [str(int(r[0])) for r in graph_rows if r[0] is not None]
            yearly_values = [float(r[1] or 0) for r in graph_rows]

            if analysis_year != "all" and analysis_year not in yearly_labels:
                analysis_year = "all"
                base_params["analysis_year"] = "all"

            table_where = scope_where
            table_params = list(scope_params)
            if analysis_year != "all":
                table_where += ' AND YEAR("Dags.greiðslu") = ?'
                table_params.append(int(analysis_year))

            breakdown_rows = []
            for child_val, amount_sum, row_count in con.execute(
                f"""
                SELECT "{child_key}" AS child_value, SUM({amount_expr}) AS amount_sum, COUNT(*) AS row_count
                FROM data
                WHERE {table_where} AND "{child_key}" IS NOT NULL
                GROUP BY "{child_key}"
                ORDER BY ABS(amount_sum) DESC NULLS LAST
                LIMIT 500
                """,
                table_params,
            ).fetchall():
                breakdown_rows.append(
                    {
                        "child_value": child_val,
                        "amount_sum": float(amount_sum or 0),
                        "amount_sum_fmt": _format_number(amount_sum),
                        "row_count": int(row_count or 0),
                        "row_count_fmt": _format_number(row_count),
                    }
                )

            record_columns = [
                c
                for c in [
                    "Dags.greiðslu",
                    parent_col,
                    child_key,
                    "Númer reiknings",
                    "Upphæð línu",
                ]
                if c
            ]

            total_records = int(
                con.execute(f"SELECT COUNT(*) FROM data WHERE {table_where}", table_params).fetchone()[0] or 0
            )
            total_pages = max(1, int(math.ceil(total_records / page_size)))
            if page > total_pages:
                page = total_pages
                base_params["page"] = page
            offset = (page - 1) * page_size

            raw_records = con.execute(
                f"""
                SELECT {", ".join([f'"{c}"' for c in record_columns])}
                FROM data
                WHERE {table_where}
                ORDER BY "Dags.greiðslu" DESC
                LIMIT ? OFFSET ?
                """,
                [*table_params, page_size, offset],
            ).fetchall()
            record_rows = []
            for row in raw_records:
                item = dict(zip(record_columns, row))
                if item.get("Upphæð línu") is not None:
                    item["Upphæð línu"] = _format_number(item["Upphæð línu"])
                record_rows.append(item)
        finally:
            con.close()

        return render_template(
            "analysis.html",
            data_loaded=True,
            parent_type=parent_type,
            parent_col=parent_col,
            child_key=child_key,
            parent_options=parent_options,
            child_keys=child_keys,
            parent_value=parent_value,
            child_value=child_value,
            analysis_year=analysis_year,
            breakdown_rows=breakdown_rows,
            record_rows=record_rows,
            record_columns=record_columns,
            yearly_labels=yearly_labels,
            yearly_values=yearly_values,
            year_links=["all"] + yearly_labels,
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            build_analysis_url=_analysis_url,
            base_params=base_params,
        )

    @app.route("/analysis/export")
    def analysis_export():
        parquet_path = Path(os.environ.get("OPNIR_PARQUET", DEFAULT_PARQUET)).resolve()
        if not parquet_path.exists():
            return Response("No data available", status=404)

        con = _open_data_connection(parquet_path)
        try:
            (
                _parent_type,
                parent_col,
                child_key,
                parent_value,
                child_value,
                _child_keys,
                _parent_options,
                analysis_year,
            ) = _analysis_scope_from_request(con)
            if not parent_value or not child_key:
                return Response("Missing selection", status=400)

            scope_params = [parent_value]
            scope_where = f'"{parent_col}" = ?'
            if child_value:
                scope_where += f' AND "{child_key}" = ?'
                scope_params.append(child_value)
            if analysis_year != "all":
                scope_where += ' AND YEAR("Dags.greiðslu") = ?'
                scope_params.append(int(analysis_year))

            cols = ["Dags.greiðslu", "Kaupandi", "Birgi", "Tegund", "Númer reiknings", "Upphæð línu"]
            raw = con.execute(
                f"SELECT {', '.join([f'"{c}"' for c in cols])} FROM data WHERE {scope_where} ORDER BY \"Dags.greiðslu\" DESC",
                scope_params,
            ).fetchall()
            rows = [dict(zip(cols, row)) for row in raw]
            for row in rows:
                if row.get("Upphæð línu") is not None:
                    row["Upphæð línu"] = _format_number(row["Upphæð línu"])
        finally:
            con.close()

        return _write_csv_response(rows, "analysis_filtered.csv")

    @app.route("/anomalies")
    def anomalies():
        anomalies_path = DEFAULT_ANOMALIES_PARQUET.resolve()
        anomalies_all_path = DEFAULT_ANOMALIES_ALL_PARQUET.resolve()
        if not anomalies_path.exists():
            return render_template(
                "anomalies.html",
                data_loaded=False,
                error=f"No anomalies file found at {anomalies_path}. Run: make anomalies",
            )

        con = _open_anomalies_connection(anomalies_path)
        con_all = _open_anomalies_all_connection(anomalies_all_path) if anomalies_all_path.exists() else con
        try:
            year = request.args.get("year", "all")
            direction = request.args.get("direction", "all")
            parent_col = request.args.get("parent_col", "Tegund")
            if parent_col not in ANOMALY_PARENT_COLUMNS:
                parent_col = "Tegund"
            parent_value = request.args.get("parent_value", "all")

            try:
                page = max(1, int(request.args.get("page", "1")))
            except ValueError:
                page = 1
            try:
                page_size = int(request.args.get("page_size", "100"))
            except ValueError:
                page_size = 100
            page_size = max(25, min(500, page_size))

            where = []
            params: list = []
            if year != "all":
                where.append("year = ?")
                params.append(int(year))
            if direction != "all":
                where.append("direction = ?")
                params.append(direction)
            if parent_value != "all":
                where.append(f'"{parent_col}" = ?')
                params.append(parent_value)
            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            total_records = int(con.execute(f"SELECT COUNT(*) FROM anomalies {where_sql}", params).fetchone()[0] or 0)
            total_pages = max(1, int(math.ceil(total_records / page_size)))
            if page > total_pages:
                page = total_pages
            offset = (page - 1) * page_size

            rows_raw = con.execute(
                f"""
                SELECT year, direction, anomaly_score, yoy_real_pct, yoy_real_change, actual_real, prior_real,
                       "Kaupandi", "Birgi", "Tegund"
                FROM anomalies
                {where_sql}
                ORDER BY (yoy_real_pct IS NULL) ASC, anomaly_score DESC, abs_change_real DESC
                LIMIT ? OFFSET ?
                """,
                [*params, page_size, offset],
            ).fetchall()
            cols = [
                "year",
                "direction",
                "anomaly_score",
                "yoy_real_pct",
                "yoy_real_change",
                "actual_real",
                "prior_real",
                "Kaupandi",
                "Birgi",
                "Tegund",
            ]
            rows = []
            for row in rows_raw:
                item = dict(zip(cols, row))
                item["anomaly_score"] = f"{float(item.get('anomaly_score') or 0):.2f}"
                pct = item.get("yoy_real_pct")
                item["yoy_real_pct_fmt"] = "N/A" if pct is None else f"{float(pct) * 100:.1f}%"
                item["yoy_real_pct_sort"] = float("-inf") if pct is None else float(pct)
                item["yoy_real_change_fmt"] = _format_number(item.get("yoy_real_change"))
                item["actual_real_fmt"] = _format_number(item.get("actual_real"))
                item["prior_real_fmt"] = _format_number(item.get("prior_real"))
                rows.append(item)

            # Pre-compute yearly sums for each series shown on the page so the UI can expand per-row details.
            key_rows = []
            seen_keys = set()
            for row in rows:
                key = (row.get("Kaupandi"), row.get("Birgi"), row.get("Tegund"))
                if key not in seen_keys:
                    seen_keys.add(key)
                    key_rows.append(key)

            totals_map: dict[tuple, list[dict]] = {}
            if key_rows:
                placeholders = ", ".join(["(?, ?, ?)"] * len(key_rows))
                flat_params = [item for key in key_rows for item in key]
                totals_rows = con_all.execute(
                    f"""
                    WITH k("Kaupandi", "Birgi", "Tegund") AS (
                        VALUES {placeholders}
                    )
                    SELECT y.year, y."Kaupandi", y."Birgi", y."Tegund", y.actual_real
                    FROM anomalies_all y
                    JOIN k
                      ON y."Kaupandi" = k."Kaupandi"
                     AND y."Birgi" = k."Birgi"
                     AND y."Tegund" = k."Tegund"
                    ORDER BY y.year
                    """,
                    flat_params,
                ).fetchall()
                for year_value, buyer_value, vendor_value, type_value, amount_value in totals_rows:
                    key = (buyer_value, vendor_value, type_value)
                    totals_map.setdefault(key, []).append(
                        {
                            "year": int(year_value),
                            "amount": float(amount_value or 0),
                            "amount_fmt": _format_number(amount_value),
                        }
                    )

            for i, row in enumerate(rows):
                key = (row.get("Kaupandi"), row.get("Birgi"), row.get("Tegund"))
                row["year_totals"] = totals_map.get(key, [])
                row["row_id"] = f"{i}:{row.get('year')}:{row.get('Kaupandi')}:{row.get('Birgi')}:{row.get('Tegund')}"

            years = [str(int(r[0])) for r in con.execute("SELECT DISTINCT year FROM anomalies ORDER BY year").fetchall()]
            parent_values = [
                r[0]
                for r in con.execute(
                    f'SELECT DISTINCT "{parent_col}" FROM anomalies WHERE "{parent_col}" IS NOT NULL ORDER BY "{parent_col}" LIMIT 1500'
                ).fetchall()
            ]

            chart_where = []
            chart_params: list = []
            if parent_value != "all":
                chart_where.append(f'"{parent_col}" = ?')
                chart_params.append(parent_value)
            chart_where_sql = f"WHERE {' AND '.join(chart_where)}" if chart_where else ""
            chart_rows = con.execute(
                f"""
                SELECT
                    year,
                    COUNT(*) AS anomalies_count,
                    SUM(abs_change_real) AS abs_change_sum,
                    SUM(CASE WHEN yoy_real_change > 0 THEN ABS(yoy_real_change) ELSE 0 END) AS abs_change_increase,
                    SUM(CASE WHEN yoy_real_change < 0 THEN ABS(yoy_real_change) ELSE 0 END) AS abs_change_decrease
                FROM anomalies
                {chart_where_sql}
                GROUP BY year
                ORDER BY year
                """,
                chart_params,
            ).fetchall()
            chart_labels = [str(int(r[0])) for r in chart_rows]
            chart_counts = [int(r[1] or 0) for r in chart_rows]
            chart_abs_change_values = [float(r[2] or 0) for r in chart_rows]
            chart_abs_increase_values = [float(r[3] or 0) for r in chart_rows]
            chart_abs_decrease_values = [float(r[4] or 0) for r in chart_rows]
            chart_abs_change = [_format_number(v) for v in chart_abs_change_values]
            chart_abs_increase = [_format_number(v) for v in chart_abs_increase_values]
            chart_abs_decrease = [_format_number(v) for v in chart_abs_decrease_values]

            base_params = {
                "year": year,
                "direction": direction,
                "parent_col": parent_col,
                "parent_value": parent_value,
                "page": page,
                "page_size": page_size,
            }
        finally:
            con.close()
            if con_all is not con:
                con_all.close()

        return render_template(
            "anomalies.html",
            data_loaded=True,
            rows=rows,
            years=years,
            directions=["all", "increase", "decrease"],
            year=year,
            direction=direction,
            parent_col=parent_col,
            parent_columns=ANOMALY_PARENT_COLUMNS,
            parent_value=parent_value,
            parent_values=parent_values,
            page=page,
            page_size=page_size,
            total_records=total_records,
            total_pages=total_pages,
            base_params=base_params,
            build_anomalies_url=_anomalies_url,
            chart_labels=chart_labels,
            chart_counts=chart_counts,
            chart_abs_change=chart_abs_change,
            chart_abs_increase=chart_abs_increase,
            chart_abs_decrease=chart_abs_decrease,
            chart_labels_json=json.dumps(chart_labels),
            chart_counts_json=json.dumps(chart_counts),
            chart_abs_change_json=json.dumps(chart_abs_change_values),
            chart_abs_increase_json=json.dumps(chart_abs_increase_values),
            chart_abs_decrease_json=json.dumps(chart_abs_decrease_values),
        )

    @app.route("/anomalies/export")
    def anomalies_export():
        anomalies_path = DEFAULT_ANOMALIES_PARQUET.resolve()
        if not anomalies_path.exists():
            return Response("No data available", status=404)

        con = _open_anomalies_connection(anomalies_path)
        try:
            year = request.args.get("year", "all")
            direction = request.args.get("direction", "all")
            parent_col = request.args.get("parent_col", "Tegund")
            if parent_col not in ANOMALY_PARENT_COLUMNS:
                parent_col = "Tegund"
            parent_value = request.args.get("parent_value", "all")

            where = []
            params: list = []
            if year != "all":
                where.append("year = ?")
                params.append(int(year))
            if direction != "all":
                where.append("direction = ?")
                params.append(direction)
            if parent_value != "all":
                where.append(f'"{parent_col}" = ?')
                params.append(parent_value)
            where_sql = f"WHERE {' AND '.join(where)}" if where else ""

            rows_raw = con.execute(
                f"""
                SELECT year, direction, anomaly_score, yoy_real_pct, yoy_real_change, actual_real, prior_real,
                       "Kaupandi", "Birgi", "Tegund"
                FROM anomalies
                {where_sql}
                ORDER BY (yoy_real_pct IS NULL) ASC, anomaly_score DESC, abs_change_real DESC
                """,
                params,
            ).fetchall()
        finally:
            con.close()

        cols = [
            "year",
            "direction",
            "anomaly_score",
            "yoy_real_pct",
            "yoy_real_change",
            "actual_real",
            "prior_real",
            "Kaupandi",
            "Birgi",
            "Tegund",
        ]
        rows = [dict(zip(cols, row)) for row in rows_raw]
        return _write_csv_response(rows, "anomalies_filtered.csv")

    @app.route("/reports")
    def reports():
        parquet_path = Path(os.environ.get("OPNIR_PARQUET", DEFAULT_PARQUET)).resolve()
        if not parquet_path.exists():
            return render_template("reports.html", data_loaded=False, error=f"No parquet file found at {parquet_path}.")

        mode = request.args.get("mode", "by_kaupandi").strip()
        if mode not in {"by_kaupandi", "by_tegund"}:
            mode = "by_kaupandi"

        buyer = request.args.get("buyer", "").strip()
        selected_tegund = request.args.get("tegund", "").strip()
        report_year = request.args.get("report_year", "all").strip()
        try:
            report_year_int = int(report_year) if report_year != "all" else None
        except ValueError:
            report_year = "all"
            report_year_int = None

        con = _open_data_connection(parquet_path)
        try:
            def _query_rows(sql: str, params: list | None = None) -> list[dict]:
                cur = con.execute(sql, params or [])
                col_names = [d[0] for d in cur.description]
                return [dict(zip(col_names, row)) for row in cur.fetchall()]

            columns = [r[0] for r in con.execute("DESCRIBE data").fetchall()]
            col_set = set(columns)
            date_col = next((c for c in ["Dags.greiðslu", "Dags.greiÃ°slu", "Dags.grei?slu"] if c in col_set), None)
            amount_col = next((c for c in ["Upphæð línu", "UpphÃ¦Ã° lÃ­nu", "Upph?? l?nu"] if c in col_set), None)
            required = {"Kaupandi", "Tegund"}
            if not required.issubset(col_set) or not date_col or not amount_col:
                return render_template("reports.html", data_loaded=False, error="Dataset is missing required columns for reports.")

            numeric_expr = f'TRY_CAST("{amount_col}" AS DOUBLE)'

            buyer_rows = _query_rows(
                f"""
                SELECT
                    "Kaupandi" AS buyer,
                    SUM({numeric_expr}) AS total_sum,
                    COUNT(*) AS row_count
                FROM data
                WHERE "Kaupandi" IS NOT NULL
                GROUP BY "Kaupandi"
                ORDER BY total_sum DESC NULLS LAST
                """
            )
            for row in buyer_rows:
                row["total_sum_fmt"] = _format_number(row.get("total_sum"))
            buyer_total_row = {
                "total_sum_fmt": _format_number(sum(float(r.get("total_sum") or 0) for r in buyer_rows)),
                "row_count": int(sum(int(r.get("row_count") or 0) for r in buyer_rows)),
            }

            tegund_options = _query_rows(
                f"""
                SELECT
                    COALESCE("Tegund", '(empty)') AS tegund,
                    SUM({numeric_expr}) AS total_sum,
                    COUNT(*) AS row_count
                FROM data
                GROUP BY COALESCE("Tegund", '(empty)')
                ORDER BY total_sum DESC NULLS LAST, tegund
                """
            )
            tegund_option_names = [str(row.get("tegund")) for row in tegund_options]

            buyer_options = set(str(row.get("buyer")) for row in buyer_rows)
            if buyer and buyer not in buyer_options:
                buyer = ""
            if selected_tegund and selected_tegund not in set(tegund_option_names):
                selected_tegund = ""

            scope_clauses = []
            scope_params: list = []
            if mode == "by_kaupandi" and buyer:
                scope_clauses.append('"Kaupandi" = ?')
                scope_params.append(buyer)
            if mode == "by_tegund" and selected_tegund:
                scope_clauses.append("COALESCE(\"Tegund\", '(empty)') = ?")
                scope_params.append(selected_tegund)
            scope_where = f"WHERE {' AND '.join(scope_clauses)}" if scope_clauses else ""
            if mode == "by_kaupandi":
                scope_label = f"Kaupandi: {buyer}" if buyer else "All kaupandi"
            else:
                scope_label = f"Tegund: {selected_tegund}" if selected_tegund else "All tegund"

            years_rows = _query_rows(
                f"""
                SELECT DISTINCT CAST(YEAR("{date_col}") AS INTEGER) AS year
                FROM data
                {scope_where}{' AND ' if scope_where else 'WHERE '}"{date_col}" IS NOT NULL
                ORDER BY year
                """,
                scope_params,
            )
            report_year_links = [str(int(row["year"])) for row in years_rows if row.get("year") is not None]

            totals_scope_clauses = list(scope_clauses)
            totals_scope_params = list(scope_params)
            if report_year_int is not None:
                totals_scope_clauses.append(f'CAST(YEAR("{date_col}") AS INTEGER) = ?')
                totals_scope_params.append(report_year_int)
            totals_scope_where = f"WHERE {' AND '.join(totals_scope_clauses)}" if totals_scope_clauses else ""

            if mode == "by_kaupandi":
                totals_rows = _query_rows(
                    f"""
                    SELECT
                        COALESCE("Tegund", '(empty)') AS group_label,
                        SUM({numeric_expr}) AS total_sum,
                        SUM(CASE WHEN {numeric_expr} > 0 THEN {numeric_expr} ELSE 0 END) AS positive_sum,
                        SUM(CASE WHEN {numeric_expr} < 0 THEN {numeric_expr} ELSE 0 END) AS negative_sum,
                        COUNT(*) AS row_count
                    FROM data
                    {totals_scope_where}
                    GROUP BY COALESCE("Tegund", '(empty)')
                    ORDER BY total_sum DESC NULLS LAST, group_label
                    """,
                    totals_scope_params,
                )
                totals_title = "Tegund totals"
                totals_first_col = "Tegund"
            else:
                totals_rows = _query_rows(
                    f"""
                    SELECT
                        "Kaupandi" AS group_label,
                        SUM({numeric_expr}) AS total_sum,
                        SUM(CASE WHEN {numeric_expr} > 0 THEN {numeric_expr} ELSE 0 END) AS positive_sum,
                        SUM(CASE WHEN {numeric_expr} < 0 THEN {numeric_expr} ELSE 0 END) AS negative_sum,
                        COUNT(*) AS row_count
                    FROM data
                    {totals_scope_where}
                    GROUP BY "Kaupandi"
                    ORDER BY total_sum DESC NULLS LAST, group_label
                    """,
                    totals_scope_params,
                )
                totals_title = "Kaupandi totals"
                totals_first_col = "Kaupandi"

            for row in totals_rows:
                row["total_sum_fmt"] = _format_number(row.get("total_sum"))
                row["positive_sum_fmt"] = _format_number(row.get("positive_sum"))
                row["negative_sum_fmt"] = _format_number(row.get("negative_sum"))
            totals_total_row = {
                "total_sum_fmt": _format_number(sum(float(r.get("total_sum") or 0) for r in totals_rows)),
                "positive_sum_fmt": _format_number(sum(float(r.get("positive_sum") or 0) for r in totals_rows)),
                "negative_sum_fmt": _format_number(sum(float(r.get("negative_sum") or 0) for r in totals_rows)),
                "row_count": int(sum(int(r.get("row_count") or 0) for r in totals_rows)),
            }

            yearly_scope_clauses = list(scope_clauses)
            yearly_scope_params = list(scope_params)
            if mode == "by_kaupandi" and selected_tegund:
                yearly_scope_clauses.append("COALESCE(\"Tegund\", '(empty)') = ?")
                yearly_scope_params.append(selected_tegund)
            if mode == "by_tegund" and buyer:
                yearly_scope_clauses.append('"Kaupandi" = ?')
                yearly_scope_params.append(buyer)
            yearly_scope_where = (
                f'WHERE {" AND ".join(yearly_scope_clauses)} AND "{date_col}" IS NOT NULL'
                if yearly_scope_clauses
                else f'WHERE "{date_col}" IS NOT NULL'
            )

            series_col = "COALESCE(\"Tegund\", '(empty)')" if mode == "by_kaupandi" else '"Kaupandi"'
            yearly_rows_raw = _query_rows(
                f"""
                SELECT
                    CAST(YEAR("{date_col}") AS INTEGER) AS year,
                    {series_col} AS series_name,
                    SUM({numeric_expr}) AS total_sum
                FROM data
                {yearly_scope_where}
                GROUP BY CAST(YEAR("{date_col}") AS INTEGER), {series_col}
                ORDER BY year, series_name
                """,
                yearly_scope_params,
            )

            year_values = sorted({int(row["year"]) for row in yearly_rows_raw if row.get("year") is not None})
            value_map: dict[tuple[int, str], float] = {}
            for row in yearly_rows_raw:
                if row.get("year") is None:
                    continue
                value_map[(int(row["year"]), str(row["series_name"]))] = float(row.get("total_sum") or 0)

            yearly_rows = []
            for row in yearly_rows_raw:
                if row.get("year") is None:
                    continue
                value = float(row.get("total_sum") or 0)
                yearly_rows.append(
                    {
                        "year": int(row["year"]),
                        "series_name": str(row["series_name"]),
                        "total_sum_fmt": _format_number(value),
                    }
                )

            series_names = sorted({str(row.get("series_name")) for row in yearly_rows_raw if row.get("series_name") is not None})
            palette = [
                "#2563eb",
                "#16a34a",
                "#dc2626",
                "#ca8a04",
                "#9333ea",
                "#0d9488",
                "#ea580c",
                "#4f46e5",
                "#059669",
                "#d946ef",
            ]
            chart_datasets = []
            for idx, name in enumerate(series_names[:12]):
                color = palette[idx % len(palette)]
                chart_datasets.append(
                    {
                        "label": name,
                        "data": [value_map.get((year, name), 0.0) for year in year_values],
                        "borderColor": color,
                        "backgroundColor": color,
                        "fill": False,
                        "tension": 0.2,
                    }
                )

            base_params = {
                "mode": mode,
                "buyer": buyer,
                "report_year": report_year,
                "tegund": selected_tegund,
            }
            return render_template(
                "reports.html",
                data_loaded=True,
                mode=mode,
                buyer=buyer,
                selected_tegund=selected_tegund,
                buyer_rows=buyer_rows,
                buyer_total_row=buyer_total_row,
                tegund_options=tegund_options,
                totals_rows=totals_rows,
                totals_total_row=totals_total_row,
                totals_title=totals_title,
                totals_first_col=totals_first_col,
                yearly_rows=yearly_rows,
                report_year=report_year,
                report_year_links=["all"] + report_year_links,
                yearly_labels_json=json.dumps([str(y) for y in year_values]),
                chart_datasets_json=json.dumps(chart_datasets),
                scope_label=scope_label,
                build_reports_url=_reports_url,
                base_params=base_params,
            )
        finally:
            con.close()


    return app


app = create_app()
