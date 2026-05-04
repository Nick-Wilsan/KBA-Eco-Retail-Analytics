import argparse
import logging
import os
import warnings
from pathlib import Path

import duckdb
import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.metrics import classification_report, confusion_matrix, precision_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")


CFG = {
    "store_id":           "STORE_1",
    "top_n_sku":          50,
    "contamination":      0.05,    
    "n_estimators":       200,
    "random_seed":        42,
    "output_dir":         "output",
    "gold_table":         "gold_mart_cold_chain_compliance",  
    "model_path":         "output/isolation_forest_model.pkl",
    "compliance_target":  95.0,    
    "temp_thresholds": {
        "frozen":   (-25.0, -15.0),  
        "chilled":  (0.0,    4.0),   
        "cool":     (8.0,   15.0),    
    },
    
    "breach_duration_min": 30,       
}

os.makedirs(CFG["output_dir"], exist_ok=True)


def load_coldchain_data(data_dir: str, store_id: str, n_sku: int) -> pd.DataFrame:
    data_dir = Path(data_dir)
    csv_files = list(data_dir.glob("*.csv"))
    assert csv_files, f"Tidak ada file CSV di {data_dir}"

    dfs = []
    for f in csv_files[:n_sku]:    # batas subset
        df = pd.read_csv(f, parse_dates=["timestamp"])
        df["sku_id"] = f.stem       
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df["store_id"] = store_id

    
    rename_map = {
        "temp": "temperature_c",
        "temperature": "temperature_c",
        "hum": "humidity_pct",
        "humidity": "humidity_pct",
        "door": "door_open_sec",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "door_open_sec" not in df.columns:
        df["door_open_sec"] = 0.0

    return df


def generate_demo_coldchain(store_id: str, n_sku: int, n_per_sku: int = 2000) -> pd.DataFrame:
    np.random.seed(CFG["random_seed"])
    zones = ["frozen", "chilled", "cool"]

    sku_meta = {}
    for i in range(1, n_sku + 1):
        zone = zones[i % 3]
        sku_meta[f"ITEM_{str(i).zfill(3)}"] = zone

    records = []
    base_ts  = pd.Timestamp.now() - pd.Timedelta(days=60)

    for sku_id, zone in sku_meta.items():
        lo, hi = CFG["temp_thresholds"][zone]
        mid    = (lo + hi) / 2
        spread = (hi - lo) / 4

        timestamps = pd.date_range(start=base_ts, periods=n_per_sku, freq="30min")

        for idx, ts in enumerate(timestamps):
            temp_ok   = np.random.normal(mid, spread)
            humid_ok  = np.random.normal(72, 6)
            door_sec  = np.random.exponential(45)    

            
            anomaly_type = None
            is_anomaly   = 0

            rand = np.random.rand()
            if rand < 0.025:   
                drift = np.random.uniform(3, 10)
                temp_ok += drift
                door_sec = np.random.uniform(30 * 60, 90 * 60)  
                anomaly_type = "equipment_breach"
                is_anomaly   = 1
            elif rand < 0.05:  
                temp_ok += np.random.choice([-1, 1]) * np.random.uniform(4, 12)
                anomaly_type = "operational_error"
                is_anomaly   = 1

            records.append({
                "store_id":     store_id,
                "sku_id":       sku_id,
                "zone":         zone,
                "timestamp":    ts,
                "temperature_c": round(float(temp_ok), 2),
                "humidity_pct":  round(float(np.clip(humid_ok, 0, 100)), 2),
                "door_open_sec": round(float(np.clip(door_sec, 0, 7200)), 1),
                "true_anomaly":  is_anomaly,
                "true_type":     anomaly_type or "normal",
            })

    return pd.DataFrame(records)



def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Buat fitur turunan dari raw sensor readings.
    Prinsip: tangkap pola gradual drift (equipment_breach) dan spike (operational_error).
    """
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["sku_id", "timestamp"]).reset_index(drop=True)

    def zone_temp_deviation(row):
        lo, hi = CFG["temp_thresholds"].get(row.get("zone", "cool"), (0, 10))
        mid = (lo + hi) / 2
        return abs(row["temperature_c"] - mid)

    def in_range(row):
        lo, hi = CFG["temp_thresholds"].get(row.get("zone", "cool"), (0, 10))
        return int(lo <= row["temperature_c"] <= hi)

    grp = df.groupby("sku_id")

    df["temp_delta"]        = grp["temperature_c"].diff().fillna(0)
    df["temp_delta_abs"]    = df["temp_delta"].abs()
    df["temp_roll_mean_6"]  = grp["temperature_c"].transform(
        lambda x: x.rolling(6, min_periods=1).mean())
    df["temp_roll_std_6"]   = grp["temperature_c"].transform(
        lambda x: x.rolling(6, min_periods=1).std().fillna(0))
    df["temp_roll_mean_24"] = grp["temperature_c"].transform(
        lambda x: x.rolling(24, min_periods=1).mean())

    
    df["temp_cumsum_3"]     = grp["temp_delta"].transform(
        lambda x: x.rolling(3, min_periods=1).sum())

    df["door_roll_max_3"]   = grp["door_open_sec"].transform(
        lambda x: x.rolling(3, min_periods=1).max())

    df["humid_roll_mean_6"] = grp["humidity_pct"].transform(
        lambda x: x.rolling(6, min_periods=1).mean())

  
    df["temp_deviation"]    = df.apply(zone_temp_deviation, axis=1)
    df["in_temp_range"]     = df.apply(in_range, axis=1)

   
    df["hour_of_day"]       = df["timestamp"].dt.hour
    df["is_night"]          = ((df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 5)).astype(int)
    df["day_of_week"]       = df["timestamp"].dt.dayofweek

    return df


FEATURE_COLS = [
    "temperature_c", "humidity_pct", "door_open_sec",
    "temp_delta", "temp_delta_abs",
    "temp_roll_mean_6", "temp_roll_std_6", "temp_roll_mean_24",
    "temp_cumsum_3", "door_roll_max_3", "humid_roll_mean_6",
    "temp_deviation", "in_temp_range",
    "hour_of_day", "is_night", "day_of_week",
]



def classify_anomaly_type(row) -> str:
    if row["anomaly_flag"] == 0:
        return "normal"
    if abs(row["temp_cumsum_3"]) > 3.0 or row["door_open_sec"] > 1800:
        return "equipment_breach"
    return "operational_error"




def train_model(df_feat: pd.DataFrame) -> tuple[IsolationForest, StandardScaler, np.ndarray]:
    X      = df_feat[FEATURE_COLS].values
    scaler = StandardScaler()
    Xs     = scaler.fit_transform(X)

    model  = IsolationForest(
        n_estimators=CFG["n_estimators"],
        contamination=CFG["contamination"],
        max_samples="auto",
        random_state=CFG["random_seed"],
        n_jobs=-1,
    )
    model.fit(Xs)


    joblib.dump({"model": model, "scaler": scaler, "features": FEATURE_COLS},
                CFG["model_path"])
    print(f"    → Model disimpan: {CFG['model_path']}")

    return model, scaler, Xs


def compute_compliance(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby(["store_id", "sku_id", "zone"])
        .agg(
            total_readings=("anomaly_flag", "count"),
            anomaly_count=("anomaly_flag", "sum"),
            equipment_breach=("anomaly_detected_type", lambda x: (x == "equipment_breach").sum()),
            operational_error=("anomaly_detected_type", lambda x: (x == "operational_error").sum()),
            avg_temp=("temperature_c", "mean"),
            avg_temp_deviation=("temp_deviation", "mean"),
            max_door_open_sec=("door_open_sec", "max"),
        )
        .reset_index()
    )
    summary["compliance_rate_pct"] = (
        (1 - summary["anomaly_count"] / summary["total_readings"]) * 100
    ).round(2)
    summary["compliant"] = summary["compliance_rate_pct"] >= CFG["compliance_target"]


    summary["anomaly_type_dominant"] = summary.apply(
        lambda r: (
            "normal" if r["anomaly_count"] == 0
            else "equipment_breach" if r["equipment_breach"] >= r["operational_error"]
            else "operational_error"
        ),
        axis=1,
    )

    return summary



def save_to_duckdb(df_detail: pd.DataFrame, df_compliance: pd.DataFrame, db_path: str):
    con = duckdb.connect(db_path)
    df_compliance["generated_at"] = pd.Timestamp.now()

    
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_fact_cold_chain (
            store_id             VARCHAR,
            sku_id               VARCHAR,
            zone                 VARCHAR,
            timestamp            TIMESTAMP,
            temperature_c        DOUBLE,
            humidity_pct         DOUBLE,
            door_open_sec        DOUBLE,
            anomaly_flag         INTEGER,
            anomaly_score        DOUBLE,
            anomaly_detected_type VARCHAR,
            risk_level           VARCHAR
        )
    """)
    con.execute("DELETE FROM silver_fact_cold_chain WHERE store_id = ?", [CFG["store_id"]])
    con.execute("INSERT INTO silver_fact_cold_chain SELECT * FROM df_detail")

   
    con.execute("""
        CREATE TABLE IF NOT EXISTS gold_mart_cold_chain_compliance (
            store_id              VARCHAR,
            sku_id                VARCHAR,
            zone                  VARCHAR,
            total_readings        BIGINT,
            anomaly_count         BIGINT,
            equipment_breach      BIGINT,
            operational_error     BIGINT,
            compliance_rate_pct   DOUBLE,
            compliant             BOOLEAN,
            anomaly_type_dominant VARCHAR,
            avg_temp              DOUBLE,
            avg_temp_deviation    DOUBLE,
            max_door_open_sec     DOUBLE,
            generated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (store_id, sku_id)
        )
    """)
    con.execute("DELETE FROM gold_mart_cold_chain_compliance WHERE store_id = ?",
                [CFG["store_id"]])
    con.execute("INSERT INTO gold_mart_cold_chain_compliance SELECT * FROM df_compliance")

    cnt = con.execute("SELECT COUNT(*) FROM gold_mart_cold_chain_compliance").fetchone()[0]
    con.close()
    print(f"    → DuckDB '{db_path}' → gold_mart_cold_chain_compliance: {cnt} SKU")


def plot_confusion(y_true, y_pred):
    cm = confusion_matrix(y_true, y_pred)
    fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=["Normal", "Anomali"],
                yticklabels=["Normal", "Anomali"], ax=ax)
    ax.set_xlabel("Prediksi Model")
    ax.set_ylabel("Ground Truth")
    ax.set_title("Confusion Matrix — Isolation Forest (Cold Chain)")
    plt.tight_layout()
    plt.savefig(os.path.join(CFG["output_dir"], "coldchain_confusion_matrix.png"), dpi=130)
    plt.close()


def plot_anomaly_scatter(df_feat: pd.DataFrame):
    sample  = df_feat.sample(min(4000, len(df_feat)), random_state=42)
    colors  = sample["anomaly_flag"].map({0: "#1a73e8", 1: "#d93025"})
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    axes[0].scatter(sample["temperature_c"], sample["anomaly_score"],
                    c=colors, alpha=0.3, s=6)
    axes[0].set_xlabel("Temperature (°C)")
    axes[0].set_ylabel("Anomaly Score")
    axes[0].set_title("Temperature vs Anomaly Score")
    axes[0].grid(True, alpha=0.3)

    axes[1].scatter(sample["door_open_sec"] / 60, sample["temp_deviation"],
                    c=colors, alpha=0.3, s=6)
    axes[1].set_xlabel("Door Open (menit)")
    axes[1].set_ylabel("Temperature Deviation from Zone Mid")
    axes[1].set_title("Door Open vs Temp Deviation")
    axes[1].grid(True, alpha=0.3)

    from matplotlib.lines import Line2D
    legend_els = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#1a73e8", ms=8, label="Normal"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#d93025", ms=8, label="Anomali"),
    ]
    axes[1].legend(handles=legend_els)
    plt.suptitle(f"Cold Chain Anomaly Detection — {CFG['store_id']}", fontsize=13)
    plt.tight_layout()
    plt.savefig(os.path.join(CFG["output_dir"], "coldchain_anomaly_scatter.png"), dpi=130)
    plt.close()


def plot_compliance_bar(df_compliance: pd.DataFrame):
    df_plot = df_compliance.sort_values("compliance_rate_pct")
    colors  = ["#d93025" if r < CFG["compliance_target"] else "#34a853"
               for r in df_plot["compliance_rate_pct"]]
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.barh(df_plot["sku_id"], df_plot["compliance_rate_pct"], color=colors, height=0.6)
    ax.axvline(CFG["compliance_target"], color="#e8710a", linestyle="--",
               linewidth=1.5, label=f"Target {CFG['compliance_target']}%")
    ax.set_xlabel("Compliance Rate (%)")
    ax.set_title(f"Cold Chain Compliance per SKU — {CFG['store_id']}")
    ax.legend()
    ax.set_xlim(80, 102)
    ax.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(CFG["output_dir"], "coldchain_compliance_bar.png"), dpi=130)
    plt.close()


def main(args):
    print("\n" + "═" * 65)
    print("  TASK 21 — Isolation Forest: Cold Chain Anomaly Detection")
    print(f"  Eco-Retail ERP Analytics | Store: {CFG['store_id']} | SKU: {CFG['top_n_sku']}")
    print("═" * 65)

 
    print(f"\n[1/5] Memuat data {'sintetis (demo)' if args.demo else 'Cold Chain Dataset'} ...")
    if args.demo:
        df = generate_demo_coldchain(CFG["store_id"], CFG["top_n_sku"])
    else:
        df = load_coldchain_data(args.data_dir, CFG["store_id"], CFG["top_n_sku"])

    print(f"    → {len(df):,} pembacaan sensor | {df['sku_id'].nunique()} SKU")
    if "true_anomaly" in df.columns:
        print(f"    → Anomali ground truth: {df['true_anomaly'].sum():,} "
              f"({df['true_anomaly'].mean()*100:.1f}%)")

    
    print(f"\n[2/5] Feature engineering ({len(FEATURE_COLS)} fitur) ...")
    df_feat = build_features(df)

    print(f"\n[3/5] Training Isolation Forest "
          f"({CFG['n_estimators']} trees, contamination={CFG['contamination']}) ...")
    model, scaler, Xs = train_model(df_feat)


    print(f"\n[4/5] Deteksi anomali ...")
    preds  = model.predict(Xs)
    scores = model.score_samples(Xs)

    df_feat["anomaly_flag"]         = (preds == -1).astype(int)
    df_feat["anomaly_score"]        = scores
    df_feat["risk_level"]           = pd.cut(
        -scores,
        bins=[0, 0.35, 0.50, 0.65, float("inf")],
        labels=["low", "medium", "high", "critical"],
    )
    df_feat["anomaly_detected_type"] = df_feat.apply(classify_anomaly_type, axis=1)

  
    if "true_anomaly" in df_feat.columns:
        y_true = df_feat["true_anomaly"]
        y_pred = df_feat["anomaly_flag"]
        prec   = precision_score(y_true, y_pred, zero_division=0) * 100
        print(f"\n  ┌─ EVALUASI MODEL ────────────────────────────────")
        print(classification_report(y_true, y_pred,
                                    target_names=["Normal", "Anomali"]))
        print(f"  │  Precision (anomali) : {prec:.1f}%  (target ≥ 80%)")
        print(f"  │  Status              : {'✅ ON TARGET' if prec >= 80 else '⚠️  PERLU TUNING'}")
        print(f"  └────────────────────────────────────────────────")
        plot_confusion(y_true, y_pred)

  
    df_compliance = compute_compliance(df_feat)
    overall_compliance = (
        1 - df_feat["anomaly_flag"].sum() / len(df_feat)
    ) * 100
    compliant_sku_pct = df_compliance["compliant"].mean() * 100
    print(f"\n  ┌─ COLD CHAIN COMPLIANCE (PRD §5 KPI) ───────────")
    print(f"  │  Overall compliance    : {overall_compliance:.2f}%  (target ≥{CFG['compliance_target']}%)")
    print(f"  │  SKU compliant         : {compliant_sku_pct:.1f}%")
    print(f"  │  Equipment breach      : {(df_feat['anomaly_detected_type']=='equipment_breach').sum():,}")
    print(f"  │  Operational error     : {(df_feat['anomaly_detected_type']=='operational_error').sum():,}")
    print(f"  └────────────────────────────────────────────────")

   
    print(f"\n[5/5] Menyimpan output ...")
    detail_cols = [
        "store_id", "sku_id", "zone", "timestamp",
        "temperature_c", "humidity_pct", "door_open_sec",
        "anomaly_flag", "anomaly_score", "anomaly_detected_type", "risk_level",
    ]
    df_detail = df_feat[detail_cols].copy()

    csv_detail = os.path.join(CFG["output_dir"], "anomaly_coldchain_output.csv")
    df_detail.to_csv(csv_detail, index=False)
    print(f"    → {csv_detail}  ({len(df_detail):,} baris)")

    csv_compliance = os.path.join(CFG["output_dir"], "coldchain_compliance_summary.csv")
    df_compliance.to_csv(csv_compliance, index=False)
    print(f"    → {csv_compliance}  ({len(df_compliance)} SKU)")

    if args.db:
        save_to_duckdb(df_detail, df_compliance, args.db)


    plot_anomaly_scatter(df_feat)
    plot_compliance_bar(df_compliance)

    print(f"\n✅  Task 21 SELESAI — output di ./{CFG['output_dir']}/")
    print(f"    Siap di-ingest Zain → silver_fact_cold_chain & gold_mart_cold_chain_compliance")
    print(f"    Model tersimpan: {CFG['model_path']} (untuk inference oleh pipeline Prefect)\n")

    return df_detail, df_compliance, model



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task 21 — Isolation Forest Cold Chain")
    parser.add_argument("--demo",     action="store_true", help="Gunakan data sintetis (tidak perlu Kaggle)")
    parser.add_argument("--data-dir", default="data/coldchain", help="Folder Cold Chain dataset")
    parser.add_argument("--db",       default=None, help="Path DuckDB, contoh: eco_retail.duckdb")
    args = parser.parse_args()

    df_detail, df_compliance, model = main(args)
