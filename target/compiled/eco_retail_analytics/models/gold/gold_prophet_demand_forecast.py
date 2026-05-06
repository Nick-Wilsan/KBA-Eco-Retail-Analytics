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

    # 1. Data Preprocessing: Aggregate by date
    df_agg = df_raw.groupby(col_date)[col_y].sum().reset_index()
    df_agg = df_agg.rename(columns={col_date: 'ds', col_y: 'y'})
    df_agg['ds'] = pd.to_datetime(df_agg['ds'])

    # 2. Zero-filling: Generate complete date range and fill missing with 0
    # This helps Prophet capture seasonality correctly instead of interpolating over missing periods
    min_date = df_agg['ds'].min()
    max_date = df_agg['ds'].max()
    all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
    df_all_dates = pd.DataFrame({'ds': all_dates})
    df_prophet = df_all_dates.merge(df_agg, on='ds', how='left').fillna({'y': 0})

    # 3. Prophet Modeling: Hyperparameter tuning to avoid underfitting
    # Increased changepoint_prior_scale makes the trend more flexible
    model_ml = Prophet(
        changepoint_prior_scale=0.05,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False
    )
    model_ml.fit(df_prophet)

    # 4. Cross-Validation
    total_days = (max_date - min_date).days
    initial_days = max(30, int(total_days * 0.5))
    
    try:
        # Evaluate model performance
        df_cv = cross_validation(model_ml, initial=f'{initial_days} days', period='30 days', horizon='30 days')
        df_p = performance_metrics(df_cv)
        mape = df_p['mape'].mean() * 100
        print(f"Prophet Cross-Validation MAPE: {mape:.2f}%")
    except Exception as e:
        print(f"Cross-validation skipped or failed: {e}")

    # Predict future (e.g., 30 days)
    future = model_ml.make_future_dataframe(periods=30)
    forecast = model_ml.predict(future)

    # 5. Visualizations
    fig1 = model_ml.plot(forecast)
    plt.title('Prophet Demand Forecast')
    plt.show()

    fig2 = model_ml.plot_components(forecast)
    plt.show()

    # 6. Database Write-back
    res = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    df_final = res.merge(df_prophet[['ds', 'y']], on='ds', how='left')
    df_final = df_final.rename(columns={
        'yhat': 'forecast_qty',
        'yhat_lower': 'lower_bound',
        'yhat_upper': 'upper_bound',
        'y': 'historical_qty'
    })

    return df_final


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"silver_fact_sales": "\"warehouse\".\"silver\".\"silver_fact_sales\""}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}
meta_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "warehouse"
    schema = "gold"
    identifier = "gold_mart_demand_forecast"
    
    def __repr__(self):
        return '"warehouse"."gold"."gold_mart_demand_forecast"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


