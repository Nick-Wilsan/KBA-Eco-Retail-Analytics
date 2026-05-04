import pandas as pd
from prophet import Prophet

def model(dbt, session):
    # Setup packages
    dbt.config(
        materialized="table",
        enabled=True,
        packages=["prophet", "pandas"]
    )

    # AMBIL DATA DARI SQL LAMA (Task 19)
    # Ini kuncinya: Python mengambil hasil dari model SQL yang sudah PASS tadi
    df_raw = dbt.ref("gold_mart_demand_forecast").to_df()

    # Persiapan data (Prophet butuh kolom 'ds' dan 'y')
    df_prophet = df_raw[['date_id', 'daily_sales_qty']].rename(
        columns={'date_id': 'ds', 'daily_sales_qty': 'y'}
    )
    
    # Training Model ML (Task 20)
    model_ml = Prophet()
    model_ml.fit(df_prophet)

    # Forecast 7 Hari (Task 23)
    future = model_ml.make_future_dataframe(periods=7)
    forecast = model_ml.predict(future)

    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
