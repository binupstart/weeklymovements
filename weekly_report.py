#!/usr/bin/env python3
"""
Weekly PL Pricing Report

Runs the mix effects query for APR, TR, Fee, Loss across fee_segment × all,
combining into a single table with WoW changes in bps.

Usage:
    python weekly_report.py                 # prints table + narrative
    python weekly_report.py --period l28    # last 28 days
    python weekly_report.py --lp-or-mpl MPL
"""

import argparse

import pandas as pd

from mix_effects import run_mix_query

REPORT_METRICS = [
    ("apr",                           "APR"),
    ("annual_target_return_rate",     "TR"),
    ("combined_fee_rate",             "Fee*"),
    ("expected_annualized_loss_rate", "Loss"),
]

SEGMENT_ORDER = ["MPL-Tprime", "MPL Full", "MPL Counter", "LP-Tprime", "LP Other"]
SEGMENT_GROUP = {
    "MPL-Tprime":  "MPL",
    "MPL Full":    "MPL",
    "MPL Counter": "MPL",
    "LP-Tprime":   "LP",
    "LP Other":    "LP",
}


def fetch_all_metrics(period, date_type, lp_or_mpl, channel, profile="DEFAULT"):
    results = {}
    common = dict(
        dim1="fee_segment", dim2="all", volume="loan_amount",
        period=period, date_type=date_type, lp_or_mpl=lp_or_mpl,
        channel=channel, investor="all", risk_grade="all", profile=profile,
    )
    for metric_key, label in REPORT_METRICS:
        print(f"  Fetching {label}...", flush=True)
        results[metric_key] = run_mix_query(metric=metric_key, **common)
    return results


def build_combined_table(results):
    base = results["apr"][["dim1", "volume_last_period", "volume_this_period",
                            "volume_share_last_period", "volume_share_this_period"]].copy()
    base.columns = ["segment", "vol_last", "vol_this", "vol_share_last", "vol_share_this"]

    for metric_key, label in REPORT_METRICS:
        col = label.replace("*", "")
        df = results[metric_key][["dim1", "metric_last_period", "metric_this_period"]].copy()
        df = df.rename(columns={"dim1": "segment",
                                 "metric_last_period": f"{col}_last",
                                 "metric_this_period": f"{col}_this"})
        base = base.merge(df, on="segment", how="left")

    total_vol_last = base["vol_last"].sum()
    total_vol_this = base["vol_this"].sum()
    total = {"segment": "Total", "vol_last": total_vol_last, "vol_this": total_vol_this,
             "vol_share_last": 1.0, "vol_share_this": 1.0}
    for metric_key, label in REPORT_METRICS:
        col = label.replace("*", "")
        mdf = results[metric_key]
        total[f"{col}_last"] = (mdf["metric_last_period"] * mdf["volume_last_period"]).sum() / mdf["volume_last_period"].sum()
        total[f"{col}_this"] = (mdf["metric_this_period"] * mdf["volume_this_period"]).sum() / mdf["volume_this_period"].sum()

    subtotal_rows = []
    for group in ["MPL", "LP"]:
        segs = [s for s in SEGMENT_ORDER if SEGMENT_GROUP.get(s) == group]
        subset = base[base["segment"].isin(segs)]
        if subset.empty:
            continue
        sub = {"segment": f"{group} Total", "vol_last": subset["vol_last"].sum(),
               "vol_this": subset["vol_this"].sum(),
               "vol_share_last": subset["vol_share_last"].sum(),
               "vol_share_this": subset["vol_share_this"].sum()}
        for metric_key, label in REPORT_METRICS:
            col = label.replace("*", "")
            mdf = results[metric_key]
            mdf_sub = mdf[mdf["dim1"].isin(segs)]
            sub[f"{col}_last"] = (mdf_sub["metric_last_period"] * mdf_sub["volume_last_period"]).sum() / mdf_sub["volume_last_period"].sum()
            sub[f"{col}_this"] = (mdf_sub["metric_this_period"] * mdf_sub["volume_this_period"]).sum() / mdf_sub["volume_this_period"].sum()
        subtotal_rows.append(sub)
    subtotals = pd.DataFrame(subtotal_rows)

    ordered = []
    prev_group = None
    for seg in SEGMENT_ORDER:
        row = base[base["segment"] == seg]
        if not row.empty:
            group = SEGMENT_GROUP.get(seg)
            if prev_group is not None and group != prev_group:
                ordered.append(subtotals[subtotals["segment"] == f"{prev_group} Total"])
            ordered.append(row)
            prev_group = group
    if prev_group:
        ordered.append(subtotals[subtotals["segment"] == f"{prev_group} Total"])
    ordered.append(pd.DataFrame([total]))

    return pd.concat(ordered, ignore_index=True)


def format_bps(val):
    return f"{round(val * 10000):+d}bps"

def format_pct(val, decimals=1):
    return f"{val * 100:.{decimals}f}%"

def format_vol(val):
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    elif val >= 1e6:
        return f"${val/1e6:.0f}m"
    return f"${val/1e3:.0f}k"


def print_table(combined, period_name):
    print(f"\nPL Pricing W/W  |  {period_name}\n")
    header = f"{'Segment':<18} {'Vol $':>8} {'% PL':>7} {'WoW%':>7}"
    for _, label in REPORT_METRICS:
        header += f"  {label.replace('*',''):>6}  {'WoW':>7}"
    print(header)
    print("-" * (len(header) + 10))

    for _, row in combined.iterrows():
        seg = str(row["segment"])
        is_total = "Total" in seg
        prefix = "  " if (seg in SEGMENT_ORDER and not is_total) else ""
        vol_wow = (row["vol_this"] / row["vol_last"] - 1) if row["vol_last"] > 0 else 0
        line = (
            f"{prefix}{seg:<18} "
            f"{format_vol(row['vol_this']):>8} "
            f"{format_pct(row['vol_share_this']):>7} "
            f"{f'{vol_wow*100:+.1f}%':>7}"
        )
        for _, label in REPORT_METRICS:
            col = label.replace("*", "")
            this_val = row.get(f"{col}_this", float("nan"))
            last_val = row.get(f"{col}_last", float("nan"))
            wow = this_val - last_val if pd.notna(this_val) and pd.notna(last_val) else float("nan")
            line += f"  {format_pct(this_val) if pd.notna(this_val) else '  -  ':>7}  {format_bps(wow) if pd.notna(wow) else '  -  ':>7}"
        if is_total:
            print("-" * (len(header) + 10))
        print(line)
    print()


def generate_narrative(combined, results, period_name):
    total_row = combined[combined["segment"] == "Total"].iloc[0]
    apr_last, apr_this = total_row["APR_last"], total_row["APR_this"]
    apr_wow_bps = round((apr_this - apr_last) * 10000)
    tr_wow_bps  = round((total_row["TR_last"]  - total_row["TR_last"])  * 10000) if pd.notna(total_row.get("TR_last"))  else 0
    fee_wow_bps = round((total_row["Fee_this"] - total_row["Fee_last"]) * 10000) if pd.notna(total_row.get("Fee_last")) else 0
    tr_wow_bps  = round((total_row["TR_this"]  - total_row["TR_last"])  * 10000) if pd.notna(total_row.get("TR_last"))  else 0

    apr_df = results["apr"].copy()
    apr_df["wow_bps"]    = (apr_df["metric_this_period"] - apr_df["metric_last_period"]) * 10000
    apr_df["vol_wow_pct"] = (apr_df["volume_this_period"] / apr_df["volume_last_period"] - 1) * 100

    biggest_price = apr_df.loc[apr_df["impact_metric_pc"].abs().idxmax()]
    biggest_vol   = apr_df.loc[apr_df["impact_volume_pc"].abs().idxmax()]

    direction = "up" if apr_wow_bps > 0 else "down"
    lines = [
        f"## Weekly Mix Effects Summary | {period_name}",
        "",
        f"### Total APR: {apr_this*100:.2f}% ({direction} {abs(apr_wow_bps)}bps WoW)",
        "",
        f"**Total WoW change: {apr_wow_bps:+d}bps** | TR: {tr_wow_bps:+d}bps | Fee: {fee_wow_bps:+d}bps",
        "",
        f"Largest price mover: **{biggest_price['dim1']}** ({biggest_price['wow_bps']:+.0f}bps WoW, {biggest_price['impact_metric_pc']*10000:+.1f}bps impact)",
        f"Largest mix mover:   **{biggest_vol['dim1']}** (vol {biggest_vol['vol_wow_pct']:+.1f}% WoW, {biggest_vol['impact_volume_pc']*10000:+.1f}bps impact)",
        "",
        "### Segment Detail (APR WoW)",
    ]
    for _, row in apr_df.sort_values("impact_metric_pc", key=abs, ascending=False).iterrows():
        lines.append(
            f"- **{row['dim1']}**: {row['wow_bps']:+.0f}bps | vol {row['vol_wow_pct']:+.1f}% | "
            f"price {row['impact_metric_pc']*10000:+.1f}bps | mix {row['impact_volume_pc']*10000:+.1f}bps"
        )
    return "\n".join(lines)


def parse_args():
    p = argparse.ArgumentParser(description="Generate PL Pricing W/W report")
    p.add_argument("--period", default="week", choices=["week", "adhoc", "dayday", "dayweek", "l28"])
    p.add_argument("--date-type", default="origination_date",
                   choices=["origination_date", "pricings_created_at_pt_date", "funding_form_submit_pt_date"])
    p.add_argument("--lp-or-mpl", default="all", choices=["all", "MPL", "LP"])
    p.add_argument("--channel", default="all", choices=["all", "onsite", "partner", "dm"])
    p.add_argument("--profile", default="DEFAULT", help="~/.databrickscfg profile to use")
    p.add_argument("--no-narrative", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    print("Fetching mix effects data for all metrics...")
    results = fetch_all_metrics(args.period, args.date_type, args.lp_or_mpl, args.channel, args.profile)
    combined = build_combined_table(results)
    period_name = results["apr"]["period_name"].iloc[0] if len(results["apr"]) else "unknown"
    print_table(combined, period_name)
    if not args.no_narrative:
        print(generate_narrative(combined, results, period_name))


if __name__ == "__main__":
    main()
