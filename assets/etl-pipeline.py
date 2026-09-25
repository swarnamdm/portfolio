"""ETL pipeline: diamonds retail data -> SQLite warehouse.
Stages: EXTRACT (CSV) -> TRANSFORM (clean, dedupe, standardize, quarantine) ->
        DATA QUALITY checks -> LOAD (SQLite: fact + summary + DQ report tables).
Run:  python3 etl_pipeline.py   (or: python3 etl_pipeline.py 2>&1 | tee etl_run_log.txt)
"""
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent
DATA = BASE.parent / "data" / "diamonds.csv"
DB = BASE / "portfolio_warehouse.db"

CUT_ORDER = ["Fair", "Good", "Very Good", "Premium", "Ideal"]
COLOR_ORDER = ["D", "E", "F", "G", "H", "I", "J"]
CLARITY_ORDER = ["FL", "IF", "VVS1", "VVS2", "VS1", "VS2", "SI1", "SI2", "I1"]

log_lines = []


def log(msg: str):
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {msg}"
    print(line, flush=True)
    log_lines.append(line)


def main():
    log("=== ETL PIPELINE START ===")

    # ---------------- EXTRACT ----------------
    log("--- EXTRACT ---")
    df = pd.read_csv(DATA)
    df.columns = df.columns.str.strip()
    n_extract = len(df)
    log(f"Extracted {n_extract:,} rows x {df.shape[1]} cols from {DATA.name}")
    log(f"Columns: {list(df.columns)}")

    # ---------------- TRANSFORM ----------------
    log("--- TRANSFORM ---")
    # 1. Type fixes: force numeric columns numeric (coerce bad values to NaN).
    for c in ["carat", "depth", "table", "price", "x", "y", "z"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    bad_numeric = int(df[["carat", "depth", "table", "price", "x", "y", "z"]].isna().sum().sum())
    log(f"Type coercion: {bad_numeric} non-numeric cells -> NaN")

    # 2. Dedupe.
    n_dupes = int(df.duplicated().sum())
    df = df.drop_duplicates().reset_index(drop=True)
    log(f"Dedupe: removed {n_dupes:,} exact duplicate rows ({n_extract:,} -> {len(df):,})")

    # 3. Standardize categories (ordered; unknown values -> NaN, reported).
    for col, order in [("cut", CUT_ORDER), ("color", COLOR_ORDER), ("clarity", CLARITY_ORDER)]:
        df[col] = df[col].astype(str).str.strip()
        unknown = sorted(set(df[col].unique()) - set(order))
        df[col] = pd.Categorical(df[col], categories=order, ordered=True)
        log(f"Standardize '{col}': {len(order)} canonical values; unknown -> NaN: {unknown or 'none'}")

    # 4. Quarantine physically-impossible dimension rows (x/y/z == 0).
    bad_dim = (df[["x", "y", "z"]] == 0).any(axis=1)
    n_bad_dim = int(bad_dim.sum())
    quarantined = df[bad_dim].copy()
    df = df[~bad_dim].reset_index(drop=True)
    log(f"Quarantine: {n_bad_dim} rows with zero x/y/z dimension excluded from fact ({len(df):,} remain)")

    # 5. Derived columns (data-engineering style enrichment).
    df["price_per_carat"] = (df["price"] / df["carat"]).round(2)
    df["volume_mm3"] = (df["x"] * df["y"] * df["z"]).round(3)
    log("Derived columns: price_per_carat, volume_mm3")
    log(f"TRANSFORM complete: {n_extract:,} -> {len(df):,} rows "
        f"({n_dupes:,} dupes + {n_bad_dim} quarantined removed)")

    # ---------------- DATA QUALITY ----------------
    log("--- DATA QUALITY CHECKS ---")
    checks = []

    def add_check(name, passed, detail):
        checks.append({"check_name": name, "passed": bool(passed), "detail": detail})
        log(f"DQ [{'PASS' if passed else 'FAIL'}] {name}: {detail}")

    # Null-rate report per column.
    for c in df.columns:
        rate = df[c].isna().mean()
        add_check(f"null_rate[{c}]", rate == 0, f"{rate * 100:.3f}% null ({int(df[c].isna().sum())} cells)")
    # Duplicate check post-transform.
    add_check("no_duplicates", df.duplicated().sum() == 0,
              f"{int(df.duplicated().sum())} duplicate rows remain")
    # Range validations.
    add_check("range[price>0]", (df["price"] > 0).all(),
              f"min=${df['price'].min():,.0f}, max=${df['price'].max():,.0f}")
    add_check("range[carat in (0,6]]", ((df["carat"] > 0) & (df["carat"] <= 6)).all(),
              f"min={df['carat'].min()}, max={df['carat'].max()}")
    add_check("range[depth 40-80]", df["depth"].between(40, 80).all(),
              f"min={df['depth'].min()}, max={df['depth'].max()}, "
              f"out-of-range={int((~df['depth'].between(40, 80)).sum())}")
    add_check("range[table 40-95]", df["table"].between(40, 95).all(),
              f"min={df['table'].min()}, max={df['table'].max()}, "
              f"out-of-range={int((~df['table'].between(40, 95)).sum())}")
    add_check("range[x,y,z > 0]", ((df[["x", "y", "z"]] > 0).all(axis=1)).all(),
              "all dimensions positive after quarantine")
    add_check("categories[cut]", df["cut"].notna().all(),
              f"{int(df['cut'].isna().sum())} unknown cut values")
    n_pass = sum(c["passed"] for c in checks)
    log(f"DQ summary: {n_pass}/{len(checks)} checks passed")

    # ---------------- LOAD ----------------
    log("--- LOAD ---")
    if DB.exists():
        DB.unlink()
        log(f"Removed existing {DB.name} for a clean load")
    con = sqlite3.connect(DB)
    df.to_sql("diamonds_clean", con, index=False)
    log(f"Loaded table diamonds_clean: {len(df):,} rows")

    cut_summary = (df.groupby("cut", observed=True)
                     .agg(n=("price", "size"),
                          median_price=("price", "median"),
                          mean_price=("price", "mean"),
                          median_price_per_carat=("price_per_carat", "median"))
                     .reset_index())
    cut_summary.to_sql("cut_summary", con, index=False)
    log(f"Loaded table cut_summary: {len(cut_summary)} rows (one per cut grade)")

    dq_report = pd.DataFrame(checks)
    dq_report.to_sql("dq_report", con, index=False)
    log(f"Loaded table dq_report: {len(dq_report)} rows (one per DQ check)")

    if n_bad_dim:
        quarantined.to_sql("quarantine_zero_dimension", con, index=False)
        log(f"Loaded table quarantine_zero_dimension: {n_bad_dim} rows (audit trail)")

    cur = con.cursor()
    tables = [r[0] for r in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    for t in tables:
        cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        log(f"Warehouse verify: {t} = {cnt:,} rows")
    con.commit()
    con.close()
    log(f"Warehouse written: {DB}")
    log("=== ETL PIPELINE END: SUCCESS ===")

    (BASE / "etl_run_log.txt").write_text("\n".join(log_lines) + "\n")
    log("Run log saved to etl_run_log.txt")


if __name__ == "__main__":
    main()
