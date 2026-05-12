import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='gold'").fetchall()
    for t in tables:
        table_name = t[0]
        print(f"--- {table_name} ---")
        cols = conn.execute(f"DESCRIBE gold.{table_name}").fetchall()
        for c in cols:
            print(f"  {c[0]} ({c[1]})")
    
    conn.close()
except Exception as e:
    print(f'Error: {e}')
