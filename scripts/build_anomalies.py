#!/usr/bin/env python3
"""Build precomputed anomalies parquet files for the Flask app."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/parquet/opnirreikningar.parquet")
    parser.add_argument("--out-flagged", default="data/parquet/anomalies_flagged.parquet")
    parser.add_argument("--out-all", default="data/parquet/anomalies_yearly_all.parquet")
    args = parser.parse_args()

    input_path = Path(args.input)
    out_flagged = Path(args.out_flagged)
    out_all = Path(args.out_all)

    if not input_path.exists():
        raise SystemExit(f"Input parquet does not exist: {input_path}")

    out_flagged.parent.mkdir(parents=True, exist_ok=True)
    out_all.parent.mkdir(parents=True, exist_ok=True)
    print(f"[anomalies] input: {input_path}")
    print(f"[anomalies] output flagged: {out_flagged}")
    print(f"[anomalies] output yearly all: {out_all}")

    con = duckdb.connect(database=":memory:")
    try:
        print("[anomalies] loading source parquet...")
        con.execute(
            "CREATE OR REPLACE VIEW data AS SELECT * FROM read_parquet(" + sql_str(str(input_path)) + ")"
        )

        base_sql = """
            WITH yearly AS (
                SELECT
                    YEAR("Dags.greiðslu") AS year,
                    "Kaupandi",
                    "Birgi",
                    "Tegund",
                    SUM(TRY_CAST("Upphæð línu" AS DOUBLE)) AS actual_real
                FROM data
                WHERE "Dags.greiðslu" IS NOT NULL
                GROUP BY 1,2,3,4
            ), with_prior AS (
                SELECT
                    year,
                    "Kaupandi",
                    "Birgi",
                    "Tegund",
                    actual_real,
                    LAG(actual_real) OVER (PARTITION BY "Kaupandi", "Birgi", "Tegund" ORDER BY year) AS prior_real
                FROM yearly
            ), with_changes AS (
                SELECT
                    year,
                    "Kaupandi",
                    "Birgi",
                    "Tegund",
                    actual_real,
                    prior_real,
                    actual_real - prior_real AS yoy_real_change,
                    CASE
                        WHEN prior_real IS NULL OR ABS(prior_real) < 0.000001 THEN NULL
                        ELSE (actual_real - prior_real) / ABS(prior_real)
                    END AS yoy_real_pct
                FROM with_prior
            ), series_counts AS (
                SELECT "Kaupandi", "Birgi", "Tegund", COUNT(*) AS series_year_count
                FROM yearly
                GROUP BY 1,2,3
            ), series_median AS (
                SELECT "Kaupandi", "Birgi", "Tegund", MEDIAN(yoy_real_pct) AS median_yoy_real_pct
                FROM with_changes
                WHERE yoy_real_pct IS NOT NULL
                GROUP BY 1,2,3
            ), series_mad AS (
                SELECT
                    c."Kaupandi",
                    c."Birgi",
                    c."Tegund",
                    MEDIAN(ABS(c.yoy_real_pct - m.median_yoy_real_pct)) AS mad_yoy_real_pct
                FROM with_changes c
                JOIN series_median m
                  ON c."Kaupandi" = m."Kaupandi"
                 AND c."Birgi" = m."Birgi"
                 AND c."Tegund" = m."Tegund"
                WHERE c.yoy_real_pct IS NOT NULL
                GROUP BY 1,2,3
            ), scored AS (
                SELECT
                    c.year,
                    c."Kaupandi",
                    c."Birgi",
                    c."Tegund",
                    c.actual_real,
                    c.prior_real,
                    c.yoy_real_change,
                    c.yoy_real_pct,
                    sc.series_year_count,
                    CASE
                        WHEN c.yoy_real_change > 0 THEN 'increase'
                        WHEN c.yoy_real_change < 0 THEN 'decrease'
                        ELSE 'flat'
                    END AS direction,
                    ABS(c.yoy_real_change) AS abs_change_real,
                    CASE
                        WHEN c.yoy_real_pct IS NULL THEN ABS(c.yoy_real_change)
                        ELSE ABS(c.yoy_real_pct)
                    END AS anomaly_score,
                    CASE
                        WHEN c.yoy_real_pct IS NULL OR mad.mad_yoy_real_pct IS NULL OR mad.mad_yoy_real_pct = 0 THEN NULL
                        ELSE 0.6745 * (c.yoy_real_pct - m.median_yoy_real_pct) / mad.mad_yoy_real_pct
                    END AS modified_zscore
                FROM with_changes c
                JOIN series_counts sc
                  ON c."Kaupandi" = sc."Kaupandi"
                 AND c."Birgi" = sc."Birgi"
                 AND c."Tegund" = sc."Tegund"
                LEFT JOIN series_median m
                  ON c."Kaupandi" = m."Kaupandi"
                 AND c."Birgi" = m."Birgi"
                 AND c."Tegund" = m."Tegund"
                LEFT JOIN series_mad mad
                  ON c."Kaupandi" = mad."Kaupandi"
                 AND c."Birgi" = mad."Birgi"
                 AND c."Tegund" = mad."Tegund"
            )
            SELECT * FROM scored
        """

        print("[anomalies] writing yearly-all parquet...")
        con.execute(
            "COPY (SELECT year, \"Kaupandi\", \"Birgi\", \"Tegund\", actual_real FROM ("
            + base_sql
            + ")) TO "
            + sql_str(str(out_all))
            + " (FORMAT PARQUET)"
        )

        flagged_sql = (
            "SELECT year, \"Kaupandi\", \"Birgi\", \"Tegund\", actual_real, prior_real, yoy_real_change, "
            "yoy_real_pct, abs_change_real, direction, anomaly_score "
            "FROM ("
            + base_sql
            + ") s "
            "WHERE prior_real IS NOT NULL "
            "AND series_year_count >= 4 "
            "AND yoy_real_pct IS NOT NULL "
            "AND ABS(yoy_real_pct) >= 0.25 "
            "AND modified_zscore IS NOT NULL "
            "AND ABS(modified_zscore) >= 3.5 "
            "AND ABS(prior_real) >= 2000000 "
            "AND ABS(yoy_real_change) >= 5000000"
        )

        print("[anomalies] writing flagged anomalies parquet...")
        con.execute(
            "COPY (" + flagged_sql + ") TO " + sql_str(str(out_flagged)) + " (FORMAT PARQUET)"
        )

        flagged_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(" + sql_str(str(out_flagged)) + ")"
        ).fetchone()[0]
        all_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(" + sql_str(str(out_all)) + ")"
        ).fetchone()[0]
        print(f"[anomalies] rows flagged: {flagged_count}")
        print(f"[anomalies] rows yearly-all: {all_count}")
        print(f"[anomalies] done: {out_flagged}")
        print(f"[anomalies] done: {out_all}")
    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
