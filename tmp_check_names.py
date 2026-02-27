import duckdb
con=duckdb.connect(':memory:')
con.execute("CREATE VIEW data AS SELECT * FROM read_parquet('data/parquet/opnirreikningar.parquet')")
rows=con.execute("SELECT DISTINCT \"Kaupandi\" FROM data WHERE \"Kaupandi\" ILIKE '%Mennt%' ORDER BY 1 LIMIT 50").fetchall()
for r in rows:
    print(repr(r[0]))
