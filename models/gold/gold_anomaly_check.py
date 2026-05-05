import pandas as pd
from sklearn.ensemble import IsolationForest

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["scikit-learn", "pandas"]
    )

    df = dbt.ref("silver_fact_cold_chain").to_df()

    if df.empty:
        return pd.DataFrame()

    features = ['temperature_c', 'humidity_percentage']
    X = df[features].fillna(0)
    
    model_if = IsolationForest(contamination=0.05, random_state=42)
    df['anomaly_score'] = model_if.fit_predict(X)

    # Membuat kolom breach (1 untuk anomali, 0 untuk normal)
    df['equipment_breach'] = df['anomaly_score'].apply(lambda x: 1 if x == -1 else 0)
    
    # Hitung persentase kepatuhan
    compliance_val = (1 - df['equipment_breach'].mean()) * 100
    df['compliance_rate_pct'] = compliance_val

    # WAJIB: Masukkan equipment_breach ke dalam return agar SQL bisa baca
    return df[[
        'telemetry_timestamp', 
        'device_id', 
        'temperature_c', 
        'equipment_breach', 
        'compliance_rate_pct'
    ]]
