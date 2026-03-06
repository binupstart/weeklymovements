#!/usr/bin/env python3
"""
Core runner for the Weekly Mix Effects query via Databricks SQL REST API.

Run directly:
    python mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade

Or import and use run_mix_query() from other scripts.
"""

import argparse
import configparser
import os
import sys
import time
from pathlib import Path

import requests
import pandas as pd
from jinja2 import Environment, FileSystemLoader

SCRIPT_DIR = Path(__file__).parent

WAREHOUSE_ID = "ef12a30cfaa1ef9b"


def get_databricks_config(profile: str = "DEFAULT") -> tuple[str, str]:
    """Read host and token from ~/.databrickscfg."""
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Databricks CLI config not found at {cfg_path}")
    parser = configparser.ConfigParser()
    # databrickscfg uses '; ' comments and may have spaces around '='
    with open(cfg_path) as f:
        content = f.read()
    # Strip inline comments (lines starting with ;)
    lines = [l for l in content.splitlines() if not l.strip().startswith(";")]
    parser.read_string("\n".join(lines))

    section = profile if profile != "DEFAULT" else "DEFAULT"
    try:
        host = parser[section].get("host", "").strip()
        token = parser[section].get("token", "").strip()
    except KeyError:
        raise ValueError(f"Profile '{profile}' not found in ~/.databrickscfg")

    if not host:
        raise ValueError(f"No 'host' found in profile '{profile}' of ~/.databrickscfg")
    if not token:
        raise ValueError(f"No 'token' found in profile '{profile}' of ~/.databrickscfg")

    return host, token


DEFAULTS = {
    "period": "week",
    "date_type": "origination_date",
    "dim1": "investor",
    "dim2": "all",
    "metric": "combined_fee_rate",
    "volume": "loan_amount",
    "lp_or_mpl": "all",
    "channel": "all",
    "investor": "all",
    "risk_grade": "all",
}

METRIC_LABELS = {
    "combined_fee_rate": "Fee",
    "annual_target_return_rate": "TR",
    "expected_annualized_loss_rate": "Loss",
    "average_loan_size": "Ave Loan",
    "apr": "APR",
    "e_share": "E Share",
    "d_per_ffs": "$/FFS",
    "is_counter_offer_flow": "Counter%",
}


def render_sql(params: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(SCRIPT_DIR)))
    template = env.get_template("query.sql.j2")
    return template.render(**params)


def run_query(
    sql: str,
    warehouse_id: str,
    host: str,
    token: str,
    poll_interval: int = 3,
) -> pd.DataFrame:
    host = host.removeprefix("https://").removeprefix("http://")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = f"https://{host}/api/2.0/sql/statements"

    resp = requests.post(
        base_url,
        headers=headers,
        json={
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        },
    )
    resp.raise_for_status()
    result = resp.json()
    statement_id = result["statement_id"]

    while result["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(poll_interval)
        resp = requests.get(f"{base_url}/{statement_id}", headers=headers)
        resp.raise_for_status()
        result = resp.json()

    if result["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"Query failed [{result['status']['state']}]: {result['status']}")

    manifest = result.get("manifest", {})
    cols = [c["name"] for c in manifest["schema"]["columns"]]
    rows = result.get("result", {}).get("data_array", [])

    df = pd.DataFrame(rows, columns=cols)

    # Cast numeric columns
    numeric_cols = [
        "metric_last_period", "metric_this_period",
        "volume_share_last_period", "volume_share_this_period",
        "volume_last_period", "volume_this_period",
        "impact_metric", "impact_volume",
        "impact_metric_pc", "impact_volume_pc",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def run_mix_query(
    metric: str = DEFAULTS["metric"],
    dim1: str = DEFAULTS["dim1"],
    dim2: str = DEFAULTS["dim2"],
    period: str = DEFAULTS["period"],
    date_type: str = DEFAULTS["date_type"],
    volume: str = DEFAULTS["volume"],
    lp_or_mpl: str = DEFAULTS["lp_or_mpl"],
    channel: str = DEFAULTS["channel"],
    investor: str = DEFAULTS["investor"],
    risk_grade: str = DEFAULTS["risk_grade"],
    profile: str = "DEFAULT",
) -> pd.DataFrame:
    """
    Render and run the mix effects query. Auth is read from ~/.databrickscfg.
    Returns a DataFrame with columns:
      period_name, dim1_name, dim1, dim2_name, dim2,
      metric_last_period, metric_this_period,
      volume_share_last_period, volume_share_this_period,
      impact_metric, impact_volume,
      impact_metric_pc, impact_volume_pc
    """
    params = {
        "period": period,
        "date_type": date_type,
        "dim1": dim1,
        "dim2": dim2,
        "metric": metric,
        "volume": volume,
        "lp_or_mpl": lp_or_mpl,
        "channel": channel,
        "investor": investor,
        "risk_grade": risk_grade,
    }
    sql = render_sql(params)
    host, token = get_databricks_config(profile)
    return run_query(sql, WAREHOUSE_ID, host, token)


def format_results(df: pd.DataFrame, metric: str, as_bps: bool = True) -> None:
    """Pretty-print the mix effects table to stdout."""
    period_name = df["period_name"].iloc[0] if len(df) else "unknown"
    label = METRIC_LABELS.get(metric, metric)
    scale = 10000 if as_bps else 100

    print(f"\n=== {label} Mix Effects | {period_name} ===")
    print(f"{'Segment':<25} {'Last':>10} {'This':>10} {'Change':>10} {'PriceImpact':>13} {'VolImpact':>11}")
    print("-" * 80)

    total_impact_metric = df["impact_metric_pc"].sum()
    total_impact_volume = df["impact_volume_pc"].sum()

    has_dim2 = df["dim2"].nunique() > 1 or (df["dim2"].iloc[0] != "all" if len(df) else False)
    sort_cols = ["dim1", "dim2"] if has_dim2 else ["dim1"]

    prev_dim1 = None
    for _, row in df.sort_values(sort_cols).iterrows():
        d1 = str(row["dim1"])
        d2 = str(row["dim2"])
        if has_dim2:
            if d1 != prev_dim1:
                if prev_dim1 is not None:
                    print()
                print(f"  [{d1}]")
                prev_dim1 = d1
            seg = f"    {d2}"
        else:
            seg = d1
        last = row["metric_last_period"]
        this = row["metric_this_period"]
        change = (this - last) * scale
        imp_m = row["impact_metric_pc"] * scale
        imp_v = row["impact_volume_pc"] * scale
        suffix = "bps" if as_bps else "%"
        print(
            f"{seg:<25} {last*100:>9.2f}% {this*100:>9.2f}% "
            f"{change:>+9.1f}{suffix} {imp_m:>+12.1f}{suffix} {imp_v:>+10.1f}{suffix}"
        )

    print("-" * 80)
    total_imp_m = df["impact_metric_pc"].sum() * scale
    total_imp_v = df["impact_volume_pc"].sum() * scale
    total_last = (df["metric_last_period"] * df["volume_last_period"]).sum() / df["volume_last_period"].sum()
    total_this = (df["metric_this_period"] * df["volume_this_period"]).sum() / df["volume_this_period"].sum()
    total_change = (total_this - total_last) * scale
    print(
        f"{'TOTAL':<25} {total_last*100:>9.2f}% {total_this*100:>9.2f}% "
        f"{total_change:>+9.1f}bps {total_imp_m:>+12.1f}bps {total_imp_v:>+10.1f}bps"
    )


def parse_args():
    p = argparse.ArgumentParser(description="Run Weekly Mix Effects query on Databricks")
    p.add_argument("--period", default=DEFAULTS["period"],
                   choices=["week", "adhoc", "dayday", "dayweek", "l28"])
    p.add_argument("--date-type", default=DEFAULTS["date_type"],
                   choices=["origination_date", "pricings_created_at_pt_date", "funding_form_submit_pt_date"])
    p.add_argument("--dim1", default=DEFAULTS["dim1"],
                   choices=["all", "risk_grade", "investor", "model", "channel", "fico",
                            "lp_or_mpl", "is_counter", "is_tprime_borrower", "fee_segment"])
    p.add_argument("--dim2", default=DEFAULTS["dim2"],
                   choices=["all", "risk_grade", "investor", "model", "channel", "fico",
                            "lp_or_mpl", "is_counter", "is_tprime_borrower", "fee_segment"])
    p.add_argument("--metric", default=DEFAULTS["metric"],
                   choices=list(METRIC_LABELS.keys()))
    p.add_argument("--volume", default=DEFAULTS["volume"],
                   choices=["loan_amount", "count", "funding_form_submits"])
    p.add_argument("--lp-or-mpl", default=DEFAULTS["lp_or_mpl"], choices=["all", "MPL", "LP"])
    p.add_argument("--channel", default=DEFAULTS["channel"], choices=["all", "onsite", "partner", "dm"])
    p.add_argument("--investor", default=DEFAULTS["investor"])
    p.add_argument("--risk-grade", default=DEFAULTS["risk_grade"], choices=["all", "A", "B", "C", "D", "E"])
    p.add_argument("--profile", default="DEFAULT", help="~/.databrickscfg profile to use")
    p.add_argument("--print-sql", action="store_true", help="Print rendered SQL and exit without running")
    p.add_argument("--output", help="Save raw results to CSV at this path")
    return p.parse_args()


def main():
    args = parse_args()

    params = {
        "period": args.period,
        "date_type": args.date_type,
        "dim1": args.dim1,
        "dim2": args.dim2,
        "metric": args.metric,
        "volume": args.volume,
        "lp_or_mpl": args.lp_or_mpl,
        "channel": args.channel,
        "investor": args.investor,
        "risk_grade": args.risk_grade,
    }

    if args.print_sql:
        print(render_sql(params))
        return

    print(f"Running: metric={args.metric}, dim1={args.dim1}, dim2={args.dim2}, period={args.period}...")
    df = run_mix_query(**{k: v for k, v in params.items()}, profile=args.profile)

    if args.output:
        df.to_csv(args.output, index=False)
        print(f"Saved to {args.output}")
    else:
        format_results(df, args.metric)


if __name__ == "__main__":
    main()
