import argparse
import logging
import os
import warnings
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from prophet import Prophet
from sklearn.metrics import mean_absolute_percentage_error

warnings.filterwarnings("ignore")
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

CFG = {
    "store_id":        "STORE_1",
    "top_n_sku":       50,
    "forecast_days":   7,
    "ci_lower":        0.10,
    "ci_upper":        0.90,
    "mape_target":     15.0,
    "min_history_days": 90,
    "wf_folds":        4,
    "random_seed":     42,
    "output_dir":      "output",
    "gold_table":      "gold_mart_demand_forecast",
}

os.makedirs(CFG["output_dir"], exist_ok=True)

def load_m5_data(data_dir: str, store_id: str, top_n: int) -> pd.DataFrame:
    """
    Load dataset M5 Forecasting (Kaggle/Walmart).
    File yang dibutuhkan di data_dir:
        - sales_train_evaluation.csv
        - calendar.csv
    """
    data_dir = Path(data_dir)
    print(f"    → Membaca sales_train_evaluation.csv ...")
    sales_df = pd.read_csv(data_dir / "sales_train_evaluation.csv")


    store_mask = sales_df["store_id"] == store_id
    sales_df   = sales_df[store_mask].copy()


    day_cols  = [c for c in sales_df.columns if c.startswith("d_")]
    sales_df["total_sales"] = sales_df[day_cols].sum(axis=1)
    top_skus  = sales_df.nlargest(top_n, "total_sales")["item_id"].tolist()
    sales_df  = sales_df[sales_df["item_id"].isin(top_skus)]

    print(f"    → Membaca calendar.csv ...")
    cal_df = pd.read_csv(data_dir / "calendar.csv", parse_dates=["date"])
    date_map = cal_df.set_index("d")["date"].to_dict()
    snap_map = cal_df.set_index("date")["snap_CA"].to_dict()


    records = []
    for _, row in sales_df.iterrows():
        for d_col in day_cols:
            date = date_map.get(d_col)
            if date is None:
                continue
            records.append({
                "store_id":   store_id,
                "sku_id":     row["item_id"],
                "date":       pd.Timestamp(date),
                "sales_qty":  int(row[d_col]),
                "is_promo":   int(snap_map.get(pd.Timestamp(date), 0)),
            })

    return pd.DataFrame(records)


def generate_demo_data(store_id: str, n_sku: int, n_days: int = 400) -> pd.DataFrame:
    """
    Data sintetis untuk demo/testing tanpa perlu download Kaggle.
    Meniru struktur & pola M5 dataset (tren + musiman mingguan + noise).
    """
    np.random.seed(CFG["random_seed"])
    dates   = pd.date_range(end=pd.Timestamp.today(), periods=n_days, freq="D")
    sku_ids = [f"FOODS_{str(i).zfill(3)}_001" for i in range(1, n_sku + 1)]

    records = []
    for sku in sku_ids:
        base      = np.random.randint(15, 120)
        trend     = np.linspace(0, np.random.uniform(-5, 20), n_days)
        weekly    = 8 * np.sin(2 * np.pi * np.arange(n_days) / 7 + np.random.uniform(0, 2))
        noise     = np.random.normal(0, base * 0.12, n_days)
        promo_idx = np.random.choice(n_days, size=int(n_days * 0.08), replace=False)
        promo     = np.zeros(n_days)
        promo[promo_idx] = np.random.uniform(5, 25, len(promo_idx))
        qty       = np.maximum(0, base + trend + weekly + noise + promo).astype(int)

        for i, d in enumerate(dates):
            records.append({
                "store_id":  store_id,
                "sku_id":    sku,
                "date":      d,
                "sales_qty": qty[i],
                "is_promo":  int(promo[i] > 0),
            })

    return pd.DataFrame(records)


def walk_forward_mape(prophet_df: pd.DataFrame, n_folds: int, forecast_days: int) -> float:
    """
    Walk-forward backtesting: split data menjadi n_folds,
    latih pada histori, prediksi ke depan, hitung rata-rata MAPE.
    """
    n = len(prophet_df)
    fold_size = n // (n_folds + 1)
    mapes = []

    for fold in range(1, n_folds + 1):
        train_end = fold * fold_size
        test_end  = min(train_end + forecast_days, n)
        if test_end <= train_end:
            break

        train = prophet_df.iloc[:train_end]
        test  = prophet_df.iloc[train_end:test_end]
        if len(test) == 0:
            continue

        m = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            seasonality_mode="multiplicative",
            changepoint_prior_scale=0.1,
            interval_width=0.80,
            uncertainty_samples=0,
        )
        if "is_promo" in train.columns:
            m.add_regressor("is_promo")
        m.fit(train)

        future   = m.make_future_dataframe(periods=len(test), freq="D")
        if "is_promo" in train.columns:
            future["is_promo"] = 0
            future.loc[future.index[-len(test):], "is_promo"] = test["is_promo"].values
        fc = m.predict(future)

        pred    = np.maximum(0, fc["yhat"].values[-len(test):])
        actual  = test["y"].values
        mape    = mean_absolute_percentage_error(actual + 1, pred + 1) * 100
        mapes.append(mape)

    return float(np.mean(mapes)) if mapes else 999.0


def train_and_forecast(df_sku: pd.DataFrame, sku_id: str) -> tuple[pd.DataFrame, float]:
    """
    Train Prophet untuk 1 SKU, lakukan walk-forward validation, return forecast.
    """
    pdf = (
        df_sku[["date", "sales_qty", "is_promo"]]
        .rename(columns={"date": "ds", "sales_qty": "y"})
        .sort_values("ds")
        .reset_index(drop=True)
    )


    mape = walk_forward_mape(pdf, n_folds=CFG["wf_folds"], forecast_days=CFG["forecast_days"])

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.1,
        interval_width=0.80,
    )
    model.add_regressor("is_promo")
    model.fit(pdf)

    future = model.make_future_dataframe(periods=CFG["forecast_days"], freq="D")
    future["is_promo"] = 0

    fc = model.predict(future)
    fc = fc[fc["ds"] > pdf["ds"].max()].copy()

    result = fc[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    result.columns = ["forecast_date", "forecast_qty", "forecast_qty_lower", "forecast_qty_upper"]
    result["forecast_qty"]       = np.maximum(0, result["forecast_qty"]).round().astype(int)
    result["forecast_qty_lower"] = np.maximum(0, result["forecast_qty_lower"]).round().astype(int)
    result["forecast_qty_upper"] = np.maximum(0, result["forecast_qty_upper"]).round().astype(int)
    result["sku_id"]             = sku_id
    result["mape_pct"]           = round(mape, 2)
    result["mape_ok"]            = mape <= CFG["mape_target"]

    return result, mape


def save_to_duckdb(df: pd.DataFrame, db_path: str):
    """
    Upsert hasil forecast ke DuckDB gold layer (PRD §4.1).
    Skema: gold_mart_demand_forecast
    """
    con = duckdb.connect(db_path)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {CFG['gold_table']} (
            store_id             VARCHAR,
            sku_id               VARCHAR,
            forecast_date        DATE,
            forecast_qty         INTEGER,
            forecast_qty_lower   INTEGER,
            forecast_qty_upper   INTEGER,
            mape_pct             DOUBLE,
            mape_ok              BOOLEAN,
            generated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (store_id, sku_id, forecast_date)
        )
    """)

    con.execute(f"DELETE FROM {CFG['gold_table']} WHERE store_id = '{CFG['store_id']}'")
    con.execute(f"INSERT INTO {CFG['gold_table']} SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {CFG['gold_table']}").fetchone()[0]
    con.close()
    print(f"    → DuckDB '{db_path}' → {CFG['gold_table']}: {count:,} baris")



def plot_sample_forecast(df_raw: pd.DataFrame, df_fc: pd.DataFrame, sku_id: str):
    actuals  = df_raw[df_raw["sku_id"] == sku_id].sort_values("date").tail(90)
    forecast = df_fc[df_fc["sku_id"] == sku_id].sort_values("forecast_date")

    fig, ax = plt.subplots(figsize=(13, 4))
    ax.plot(actuals["date"], actuals["sales_qty"],
            label="Aktual (90 hari terakhir)", color="#1a73e8", linewidth=1)
    ax.plot(forecast["forecast_date"], forecast["forecast_qty"],
            label=f"Forecast {CFG['forecast_days']}H", color="#e8710a",
            linewidth=2, linestyle="--", marker="o", markersize=4)
    ax.fill_between(
        forecast["forecast_date"],
        forecast["forecast_qty_lower"],
        forecast["forecast_qty_upper"],
        alpha=0.2, color="#e8710a", label="CI 80%",
    )
    mape_val = forecast["mape_pct"].iloc[0]
    status   = "✅ ON TARGET" if mape_val <= CFG["mape_target"] else "⚠️ PERLU TUNING"
    ax.set_title(
        f"Prophet Demand Forecast — {sku_id} | Store: {CFG['store_id']}\n"
        f"Walk-forward MAPE: {mape_val:.2f}% {status}  (target ≤{CFG['mape_target']}%)"
    )
    ax.set_xlabel("Tanggal")
    ax.set_ylabel("Sales Qty")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = os.path.join(CFG["output_dir"], f"forecast_sample_{sku_id}.png")
    plt.savefig(out, dpi=130)
    plt.close()
    print(f"    → Plot tersimpan: {out}")



def main(args):
    print("\n" + "═" * 65)
    print("  TASK 20 — Prophet Demand Forecast")
    print(f"  Eco-Retail ERP Analytics | Store: {CFG['store_id']} | SKU: {CFG['top_n_sku']}")
    print("═" * 65)


    print(f"\n[1/4] Memuat data {'sintetis (demo)' if args.demo else 'M5 dataset'} ...")
    if args.demo:
        df = generate_demo_data(CFG["store_id"], CFG["top_n_sku"])
    else:
        df = load_m5_data(args.data_dir, args.store, CFG["top_n_sku"])

    print(f"    → {len(df):,} baris | {df['sku_id'].nunique()} SKU "
          f"| {df['date'].min().date()} – {df['date'].max().date()}")


    hist_days = (df["date"].max() - df["date"].min()).days
    assert hist_days >= CFG["min_history_days"], \
        f"Histori hanya {hist_days} hari, minimal {CFG['min_history_days']} hari diperlukan."

    print(f"\n[2/4] Training Prophet + Walk-Forward Validation per SKU ...")
    all_fc, mape_log, failed = [], {}, []
    sku_list = df["sku_id"].unique()

    for i, sku in enumerate(sku_list, 1):
        try:
            fc, mape = train_and_forecast(df[df["sku_id"] == sku], sku)
            fc["store_id"] = CFG["store_id"]
            all_fc.append(fc)
            mape_log[sku] = mape
            status = "✓" if mape <= CFG["mape_target"] else "✗"
            print(f"    [{i:02d}/{len(sku_list)}] {sku}  MAPE(WF): {mape:.2f}%  {status}")
        except Exception as e:
            failed.append(sku)
            print(f"    [{i:02d}/{len(sku_list)}] {sku}  GAGAL: {e}")

    df_fc = pd.concat(all_fc, ignore_index=True)
    df_fc  = df_fc[[
        "store_id", "sku_id", "forecast_date",
        "forecast_qty", "forecast_qty_lower", "forecast_qty_upper",
        "mape_pct", "mape_ok",
    ]]


    print(f"\n[3/4] Ringkasan hasil ...")
    s = pd.Series(mape_log)
    print(f"\n  ┌─ MAPE WALK-FORWARD VALIDATION ───────────────────")
    print(f"  │  Median MAPE   : {s.median():.2f}%")
    print(f"  │  Mean MAPE     : {s.mean():.2f}%")
    print(f"  │  Best SKU      : {s.idxmin()} ({s.min():.2f}%)")
    print(f"  │  Worst SKU     : {s.idxmax()} ({s.max():.2f}%)")
    pct_ok = (s <= CFG["mape_target"]).mean() * 100
    print(f"  │  On-target     : {pct_ok:.1f}% SKU (target ≤{CFG['mape_target']}%)")
    print(f"  │  Gagal training: {len(failed)}")
    print(f"  └───────────────────────────────────────────────────")

    print(f"\n[4/4] Menyimpan output ...")


    csv_path = os.path.join(CFG["output_dir"], "demand_forecast_output.csv")
    df_fc.to_csv(csv_path, index=False)
    print(f"    → {csv_path}  ({len(df_fc):,} baris)")

    mape_df = s.reset_index()
    mape_df.columns = ["sku_id", "mape_pct"]
    mape_df["on_target"] = mape_df["mape_pct"] <= CFG["mape_target"]
    mape_df.to_csv(os.path.join(CFG["output_dir"], "mape_walkforward_summary.csv"), index=False)

    if args.db:
        save_to_duckdb(df_fc, args.db)

    
    plot_sample_forecast(df, df_fc, sku_list[0])

    print(f"\n✅  Task 20 SELESAI — output di ./{CFG['output_dir']}/")
    print(f"    Siap di-ingest Zain ke pipeline → {CFG['gold_table']}\n")

    return df_fc, mape_log


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Task 20 — Prophet Demand Forecast")
    parser.add_argument("--demo",      action="store_true",  help="Gunakan data sintetis (tidak perlu Kaggle)")
    parser.add_argument("--data-dir",  default="data/m5",    help="Folder dataset M5 (default: data/m5)")
    parser.add_argument("--store",     default="STORE_1",    help="Store ID (sesuai M5: STORE_1..STORE_10)")
    parser.add_argument("--db",        default=None,         help="Path DuckDB, contoh: eco_retail.duckdb")
    args = parser.parse_args()

    if not args.demo:
        CFG["store_id"] = args.store

    df_fc, mape_log = main(args)
