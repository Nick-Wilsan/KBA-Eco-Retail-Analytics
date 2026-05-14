import duckdb

try:
    conn = duckdb.connect('data/warehouse.duckdb', read_only=True)
    
    # 1. Food Waste Rate
    print('--- Food Waste Rate ---')
    q1 = '''
        SELECT 
            SUM(unsold_qty) * 100.0 / SUM(total_stock) AS waste_rate_pct_calc,
            SUM(unsold_qty) * 1.0 / SUM(total_stock) AS waste_rate_ratio
        FROM gold.gold_mart_food_waste_summary
    '''
    print(conn.execute(q1).fetchdf())

    # 2. Trend Waste Rate
    print('\n--- Trend Food Waste Rate (Sample) ---')
    q2 = '''
        SELECT date_id, SUM(unsold_qty) * 1.0 / SUM(total_stock) AS ratio
        FROM gold.gold_mart_food_waste_summary
        GROUP BY date_id
        ORDER BY date_id
        LIMIT 5
    '''
    print(conn.execute(q2).fetchdf())

    # 3. Cold Chain Breach Timeline
    print('\n--- Cold Chain Breach ---')
    q3 = '''
        SELECT date_id, SUM(equipment_breach_count) AS breaches
        FROM gold.gold_mart_cold_chain_compliance
        WHERE equipment_breach_count > 0
        GROUP BY date_id
        ORDER BY date_id
    '''
    print(conn.execute(q3).fetchdf())

    conn.close()
except Exception as e:
    print(f'Error: {e}')
