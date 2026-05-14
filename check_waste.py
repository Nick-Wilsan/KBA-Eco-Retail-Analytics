import duckdb
try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    print('--- Checking Overlap and Waste Rate Fluctuations ---')
    print('Testing Trend Query:')
    q = '''
        SELECT date_id, SUM(unsold_qty) * 100.0 / SUM(total_stock) AS avg_waste_rate
        FROM gold.gold_mart_food_waste_summary
        GROUP BY date_id
        HAVING SUM(total_sold) > 0 -- Ensure we only look at days where sales actually happened
        ORDER BY date_id
        LIMIT 10
    '''
    print(conn.execute(q).fetchdf())
    conn.close()
except Exception as e:
    print(f'Error: {e}')
