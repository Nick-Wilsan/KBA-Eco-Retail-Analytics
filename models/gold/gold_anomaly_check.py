import argparse
import os
import sys
import textwrap
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_task20(demo: bool, data_dir: str, store: str, db: str | None):
  
    print("\n" + "▶" * 3 + "  TASK 20 — Prophet Demand Forecast")

    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "task20", pathlib.Path(__file__).parent
    )
    t20 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(t20)

    class Args20:
        pass
    args = Args20()
    args.demo     = demo
    args.data_dir = data_dir
    args.store    = store
    args.db       = db

    if not demo:
        t20.CFG["store_id"] = store

    df_fc, mape_log = t20.main(args)
    return df_fc, mape_log


def run_task21(demo: bool, data_dir: str, db: str | None):
    print("\n" + "▶" * 3 + "  TASK 21 — Isolation Forest Cold Chain")
    import importlib.util, pathlib
    spec = importlib.util.spec_from_file_location(
        "task21", pathlib.Path(__file__).parent
    )
    t21 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(t21)

    class Args21:
        pass
    args = Args21()
    args.demo     = demo
    args.data_dir = data_dir
    args.db       = db

    df_detail, df_compliance, model = t21.main(args)
    return df_detail, df_compliance



def load_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fc_path  = os.path.join(OUTPUT_DIR, "demand_forecast_output.csv")
    ano_path = os.path.join(OUTPUT_DIR, "anomaly_coldchain_output.csv")
    com_path = os.path.join(OUTPUT_DIR, "coldchain_compliance_summary.csv")

    missing = [p for p in [fc_path, ano_path, com_path] if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            f"File berikut tidak ditemukan:\n  " + "\n  ".join(missing) +
            "\nJalankan dengan --demo atau pastikan Task 20 & 21 sudah selesai."
        )

    df_fc   = pd.read_csv(fc_path,  parse_dates=["forecast_date"])
    df_ano  = pd.read_csv(ano_path, parse_dates=["timestamp"])
    df_com  = pd.read_csv(com_path)
    return df_fc, df_ano, df_com


def build_executive_kpi(
    df_fc:  pd.DataFrame,
    df_ano: pd.DataFrame,
    df_com: pd.DataFrame,
    store_id: str,
) -> pd.DataFrame:
    today = datetime.now().date()


    mape_series     = df_fc.groupby("sku_id")["mape_pct"].first()
    demand_forecast_7d = df_fc["forecast_qty"].sum()
    mape_median     = mape_series.median()
    mape_on_target  = (mape_series <= 15.0).mean() * 100


    overall_compliance = (
        1 - df_ano["anomaly_flag"].sum() / len(df_ano)
    ) * 100 if len(df_ano) > 0 else 0
    total_breach  = (df_ano.get("anomaly_detected_type", pd.Series()) == "equipment_breach").sum()
    total_op_err  = (df_ano.get("anomaly_detected_type", pd.Series()) == "operational_error").sum()


    def traffic_light(value, green_threshold, red_threshold, higher_is_better=True):
        if higher_is_better:
            if value >= green_threshold: return "green"
            if value >= red_threshold:   return "yellow"
            return "red"
        else:
            if value <= green_threshold: return "green"
            if value <= red_threshold:   return "yellow"
            return "red"

    kpi = {
        "store_id":                   store_id,
        "snapshot_date":              today,
        "demand_forecast_next7d_qty": int(demand_forecast_7d),
        "demand_mape_median_pct":     round(mape_median, 2),
        "demand_mape_on_target_pct":  round(mape_on_target, 2),
        "demand_status":              traffic_light(mape_median, 10, 15, higher_is_better=False),

        "coldchain_compliance_pct":   round(overall_compliance, 2),
        "coldchain_anomaly_total":    int(df_ano["anomaly_flag"].sum()),
        "coldchain_equipment_breach": int(total_breach),
        "coldchain_operational_error":int(total_op_err),
        "coldchain_status":           traffic_light(overall_compliance, 95, 88),

        "sku_forecast_count":         int(df_fc["sku_id"].nunique()),
        "sku_coldchain_count":        int(df_com["sku_id"].nunique()),
        "sku_critical_count":         int(
            df_com[df_com["compliance_rate_pct"] < 88]["sku_id"].nunique()
        ) if "compliance_rate_pct" in df_com.columns else 0,
    
        "generated_at":               datetime.now().isoformat(),
    }

    return pd.DataFrame([kpi])


def build_alert_table(df_fc: pd.DataFrame, df_com: pd.DataFrame) -> pd.DataFrame:
  
    fc_agg = (
        df_fc.groupby("sku_id")
        .agg(
            avg_forecast_qty=("forecast_qty", "mean"),
            max_forecast_qty=("forecast_qty", "max"),
            mape_pct=("mape_pct", "first"),
            mape_ok=("mape_ok", "first"),
        )
        .reset_index()
    )


    merged = fc_agg.merge(
        df_com[["sku_id", "zone", "compliance_rate_pct", "compliant",
                "equipment_breach", "operational_error"]],
        on="sku_id", how="outer"
    )


    def classify(row):
        has_breach = pd.notna(row.get("equipment_breach")) and row.get("equipment_breach", 0) > 0
        compliance  = row.get("compliance_rate_pct", 100)
        mape        = row.get("mape_pct", 0)
        demand      = row.get("avg_forecast_qty", 0)

        if has_breach and compliance < 88:
            return "KRITIKAL"
        if compliance < 95 or mape > 20:
            return "WASPADA"
        if mape > 15:
            return "MONITOR_FORECAST"
        if compliance < 97:
            return "MONITOR_COLDCHAIN"
        return "AMAN"

    merged["alert_status"] = merged.apply(classify, axis=1)
    return merged.sort_values(
        ["alert_status", "avg_forecast_qty"], ascending=[True, False]
    ).reset_index(drop=True)


def save_all_to_duckdb(
    df_kpi:   pd.DataFrame,
    df_alert: pd.DataFrame,
    db_path:  str,
):
    con = duckdb.connect(db_path)

    con.execute("""
        CREATE TABLE IF NOT EXISTS gold_mart_executive_kpi (
            store_id                    VARCHAR,
            snapshot_date               DATE,
            demand_forecast_next7d_qty  BIGINT,
            demand_mape_median_pct      DOUBLE,
            demand_mape_on_target_pct   DOUBLE,
            demand_status               VARCHAR,
            coldchain_compliance_pct    DOUBLE,
            coldchain_anomaly_total     BIGINT,
            coldchain_equipment_breach  BIGINT,
            coldchain_operational_error BIGINT,
            coldchain_status            VARCHAR,
            sku_forecast_count          INTEGER,
            sku_coldchain_count         INTEGER,
            sku_critical_count          INTEGER,
            generated_at                VARCHAR,
            PRIMARY KEY (store_id, snapshot_date)
        )
    """)
    con.execute("DELETE FROM gold_mart_executive_kpi WHERE store_id = ?",
                [df_kpi["store_id"].iloc[0]])
    con.execute("INSERT INTO gold_mart_executive_kpi SELECT * FROM df_kpi")


    con.execute("DROP TABLE IF EXISTS gold_mart_sku_alert")
    con.execute("CREATE TABLE gold_mart_sku_alert AS SELECT * FROM df_alert")


    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'gold_%'"
    ).fetchall()
    print(f"    → DuckDB '{db_path}' — Gold Tables:")
    for (t,) in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"       • {t}: {cnt} baris")

    con.close()



def generate_report(df_kpi: pd.DataFrame, df_alert: pd.DataFrame) -> str:
    kpi = df_kpi.iloc[0]
    alert_counts = df_alert["alert_status"].value_counts().to_dict()
    top_critical  = df_alert[df_alert["alert_status"] == "KRITIKAL"].head(5)

    report = textwrap.dedent(f"""

    • Total prediksi demand 7 hari ke depan : {int(kpi['demand_forecast_next7d_qty']):,} unit
    • MAPE Median (walk-forward)            : {kpi['demand_mape_median_pct']:.2f}%
    • SKU on-target (MAPE ≤15%)             : {kpi['demand_mape_on_target_pct']:.1f}%
    • Status                                : {kpi['demand_status'].upper()} ({'✅' if kpi['demand_status']=='green' else '⚠️'})
    • PRD Target MAPE                       : ≤ 15%

    
   
    • Overall compliance rate  : {kpi['coldchain_compliance_pct']:.2f}%
    • PRD Target compliance    : ≥ 95%
    • Status                   : {kpi['coldchain_status'].upper()} ({'✅' if kpi['coldchain_status']=='green' else '⚠️'})
    • Total anomali terdeteksi : {int(kpi['coldchain_anomaly_total']):,}
      - Equipment breach       : {int(kpi['coldchain_equipment_breach']):,}  (suhu naik gradual >30 menit)
      - Operational error      : {int(kpi['coldchain_operational_error']):,} (lonjakan sementara)
    • SKU dalam kondisi KRITIKAL: {int(kpi['sku_critical_count'])}

    • AMAN              : {alert_counts.get('AMAN', 0)} SKU
    • MONITOR FORECAST  : {alert_counts.get('MONITOR_FORECAST', 0)} SKU
    • MONITOR COLDCHAIN : {alert_counts.get('MONITOR_COLDCHAIN', 0)} SKU
    • WASPADA           : {alert_counts.get('WASPADA', 0)} SKU
    • KRITIKAL          : {alert_counts.get('KRITIKAL', 0)} SKU

  
    {top_critical[['sku_id','alert_status','avg_forecast_qty','mape_pct','compliance_rate_pct']].to_string(index=False) if len(top_critical) > 0 else '    (Tidak ada SKU KRITIKAL — sistem dalam kondisi baik)'}

    """).strip()

    return report



def run_ml_pipeline(
    demo:     bool = True,
    db_path:  str | None = None,
    data_dir_m5:      str = "data/m5",
    data_dir_coldchain: str = "data/coldchain",
    store_id: str = "STORE_1",
    run_models: bool = True,
) -> dict:
    
    if run_models:
        df_fc,  mape_log     = run_task20(demo, data_dir_m5, store_id, db_path)
        df_ano, df_com       = run_task21(demo, data_dir_coldchain, db_path)
    else:
        df_fc, df_ano, df_com = load_outputs()

    df_kpi   = build_executive_kpi(df_fc, df_ano, df_com, store_id)
    df_alert = build_alert_table(df_fc, df_com)

    if db_path:
        save_all_to_duckdb(df_kpi, df_alert, db_path)

    report = generate_report(df_kpi, df_alert)
    report_path = os.path.join(OUTPUT_DIR, "pipeline_summary_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    df_alert.to_csv(os.path.join(OUTPUT_DIR, "pipeline_alert_skus.csv"), index=False)

    print(report)
    print(f"\n✅  Task 22 SELESAI")
    print(f"    Laporan : {report_path}")
    print(f"    Alert   : output/pipeline_alert_skus.csv")

    return {
        "df_forecast":   df_fc,
        "df_anomaly":    df_ano,
        "df_compliance": df_com,
        "df_kpi":        df_kpi,
        "df_alert":      df_alert,
    }



def main():
    print("\n" + "═" * 65)
    print("  TASK 22 — ML Pipeline Integrator")
    print("  Eco-Retail ERP Analytics | Kelompok 8")
    print("═" * 65)

    parser = argparse.ArgumentParser(description="Task 22 — ML Pipeline Integrator")
    parser.add_argument("--demo",        action="store_true", help="Gunakan data sintetis")
    parser.add_argument("--no-run-models", action="store_true",
                        help="Jangan jalankan ulang Task 20 & 21, pakai CSV yang sudah ada")
    parser.add_argument("--db",          default=None, help="Path DuckDB, contoh: eco_retail.duckdb")
    parser.add_argument("--store",       default="STORE_1")
    args = parser.parse_args()

    run_ml_pipeline(
        demo=args.demo,
        db_path=args.db,
        store_id=args.store,
        run_models=not args.no_run_models,
    )


if __name__ == "__main__":
    main()
