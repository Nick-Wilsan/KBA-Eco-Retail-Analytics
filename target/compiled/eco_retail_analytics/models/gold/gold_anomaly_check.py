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


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"silver_fact_cold_chain": "\"warehouse\".\"silver\".\"silver_fact_cold_chain\""}
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
    identifier = "gold_anomaly_check"
    
    def __repr__(self):
        return '"warehouse"."gold"."gold_anomaly_check"'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


