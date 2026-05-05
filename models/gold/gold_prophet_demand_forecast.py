import pandas as pd
from prophet import Prophet
import numpy as np

def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["prophet", "pandas", "numpy"]
    )

    df_raw = dbt.ref("silver_fact_sales").to_df()
    if df_raw.empty:
        return pd.DataFrame()

    # DETEKSI KOLOM OTOMATIS
    col_date = next((c for c in ['date', 'order_date_id', 'ds'] if c in df_raw.columns), None)
    col_y = next((c for c in ['sales_qty', 'quantity', 'qty', 'total_items_sold'] if c in df_raw.columns), None)
    col_prod = next((c for c in ['sku_id', 'product_id'] if c in df_raw.columns), None)

    final_forecasts = []
    
    # Kita hanya proses kombinasi store & product
    groups = df_raw.groupby(['store_id', col_prod])

    for (store, prod_id), group in groups:
        df_prophet = group[[col_date, col_y]].rename(
            columns={col_date: 'ds', col_y: 'y'}
        )
        
        # PASTIKAN DATA CUKUP (Minimal 5-10 baris agar Prophet tidak error)
        if len(df_prophet) > 5:
            try:
                # Kita matikan ketidakpastian (uncertainty) untuk mempercepat dan menghindari error broadcast
                model_ml = Prophet(
                    daily_seasonality=True, 
                    uncertainty_samples=0 # Ini kunci agar tidak error 'broadcast shapes'
                )
                model_ml.fit(df_prophet)
                
                future = model_ml.make_future_dataframe(periods=7)
                forecast = model_ml.predict(future)
                
                # Ambil hasil prediksi saja
                res = forecast[['ds', 'yhat']].tail(7)
                # Karena uncertainty_samples=0, kita buat manual lower/upper bound sederhana
                res['yhat_lower'] = res['yhat'] * 0.95
                res['yhat_upper'] = res['yhat'] * 1.05
                
                res['store_id'] = store
                res[col_prod] = prod_id
                final_forecasts.append(res)
            except:
                continue # Jika satu produk gagal, lanjut ke produk berikutnya

    if not final_forecasts:
        return pd.DataFrame(columns=['ds', 'forecast_qty', 'lower_bound', 'upper_bound', 'store_id', col_prod])

    df_final = pd.concat(final_forecasts, ignore_index=True)
    
    # Rename sesuai request Nick
    return df_final.rename(columns={
        'yhat': 'forecast_qty',
        'yhat_lower': 'lower_bound',
        'yhat_upper': 'upper_bound'
    })
