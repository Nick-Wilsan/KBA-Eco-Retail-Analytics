import pandas as pd
import duckdb
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
import warnings
import logging

# Menyembunyikan warning dan log cmdstanpy agar output terminal bersih
warnings.filterwarnings('ignore')
logger = logging.getLogger('cmdstanpy')
logger.addHandler(logging.NullHandler())
logger.propagate = False
logger.setLevel(logging.CRITICAL)

def run_task2():
    print("="*60)
    print("MENGHITUNG MAPE (PROPHET CROSS-VALIDATION)")
    print("="*60)
    
    conn = duckdb.connect("data/warehouse.duckdb", read_only=True)
    df_sales = conn.execute("SELECT * FROM silver.silver_fact_sales").fetchdf()
    
    df_agg = df_sales.groupby('order_date_id')['quantity'].sum().reset_index()
    df_agg = df_agg.rename(columns={'order_date_id': 'ds', 'quantity': 'y'})
    df_agg['ds'] = pd.to_datetime(df_agg['ds'])
    
    min_date = df_agg['ds'].min()
    max_date = df_agg['ds'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    df_all_dates = pd.DataFrame({'ds': all_dates})
    df_prophet = df_all_dates.merge(df_agg, on='ds', how='left').fillna({'y': 0})
    
    model_ml = Prophet(
        changepoint_prior_scale=0.10,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    # Fit model (output stan disembunyikan)
    model_ml.fit(df_prophet)
    
    total_days = (max_date - min_date).days
    initial_days = max(30, int(total_days * 0.5))
    
    # Progress bar CV tetap akan muncul secara default oleh Prophet
    df_cv = cross_validation(model_ml, initial=f'{initial_days} days', period='30 days', horizon='30 days')
    df_p = performance_metrics(df_cv)
    mape = df_p['mape'].mean() * 100
    
    print("\n" + "="*60)
    print(f"Hasil Evaluasi Model Prophet:")
    print(f"Prophet Cross-Validation MAPE: {mape:.2f}%")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_task2()