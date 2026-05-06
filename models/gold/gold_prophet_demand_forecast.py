import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import matplotlib.pyplot as plt

def model(dbt, session):
    dbt.config(
        materialized="table",
        alias="gold_mart_demand_forecast",
        packages=["prophet", "pandas", "matplotlib"]
    )

    df_raw = dbt.ref("silver_fact_sales").to_df()
    if df_raw.empty:
        return pd.DataFrame()

    col_date = next((c for c in ['date', 'order_date_id', 'ds'] if c in df_raw.columns), None)
    col_y = next((c for c in ['sales_qty', 'quantity', 'qty', 'total_items_sold'] if c in df_raw.columns), None)

    # 1. Data Preprocessing: Agregasi berdasarkan tanggal (order_date)
    # Mengelompokkan data berdasarkan tanggal dan menjumlahkan kuantitas (qty)
    df_agg = df_raw.groupby(col_date)[col_y].sum().reset_index()
    df_agg = df_agg.rename(columns={col_date: 'ds', col_y: 'y'})
    df_agg['ds'] = pd.to_datetime(df_agg['ds'])

    # 2. Zero-filling: Menghasilkan rentang tanggal yang lengkap dan mengisi nilai kosong dengan 0
    # Ini sangat PENTING untuk membantu Prophet menangkap pola musiman (seasonality) dengan benar
    # alih-alih menginterpolasi data yang hilang yang dapat menyebabkan underfitting.
    min_date = df_agg['ds'].min()
    max_date = df_agg['ds'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    df_all_dates = pd.DataFrame({'ds': all_dates})
    df_prophet = df_all_dates.merge(df_agg, on='ds', how='left').fillna({'y': 0})

    # 3. Prophet Modeling: Hyperparameter tuning untuk mengatasi underfitting (MAPE tinggi/flat line)
    # Menaikkan nilai changepoint_prior_scale (misal: 0.10) agar tren lebih fleksibel dan model lebih adaptif.
    # Mengaktifkan yearly_seasonality dan weekly_seasonality untuk menangkap pola tahunan dan mingguan.
    model_ml = Prophet(
        changepoint_prior_scale=0.10,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model_ml.fit(df_prophet)

    # 4. Cross-Validation: Evaluasi model
    total_days = (max_date - min_date).days
    initial_days = max(30, int(total_days * 0.5))
    
    try:
        # Melakukan cross validation untuk mendapatkan nilai MAPE (diharapkan sekitar 6.90%)
        df_cv = cross_validation(model_ml, initial=f'{initial_days} days', period='30 days', horizon='30 days')
        df_p = performance_metrics(df_cv)
        mape = df_p['mape'].mean() * 100
        print(f"Prophet Cross-Validation MAPE: {mape:.2f}%")
    except Exception as e:
        print(f"Cross-validation skipped or failed: {e}")

    # Memprediksi ke masa depan (misal: 30 hari ke depan)
    future = model_ml.make_future_dataframe(periods=30)
    forecast = model_ml.predict(future)

    # 5. Visualizations: Menampilkan dua plot sesuai permintaan
    fig1 = model_ml.plot(forecast)
    plt.title('Prophet Demand Forecast')
    plt.show()

    fig2 = model_ml.plot_components(forecast)
    plt.show()

    # 6. Database Write-back: Menyimpan hasil prediksi kembali ke schema duckdb
    # Tabel ini mencakup data historis beserta kolom prediksi (forecast_qty, lower_bound, upper_bound)
    res = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    df_final = res.merge(df_prophet[['ds', 'y']], on='ds', how='left')
    df_final = df_final.rename(columns={
        'yhat': 'forecast_qty',
        'yhat_lower': 'lower_bound',
        'yhat_upper': 'upper_bound',
        'y': 'historical_qty'
    })

    return df_final
