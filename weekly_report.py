#!/usr/bin/env python3
"""
Weekly PL Pricing Report

Replicates the "PL Pricing W/W" slide by running the mix effects query for
multiple metrics (APR, TR, Fee, Loss) across fee_segment × all, then combining
into a single table with WoW changes in bps.

Usage:
    python weekly_report.py                              # prints table + narrative
    python weekly_report.py --period l28                # last 28 days
    python weekly_report.py --output report.png         # save chart image
    python weekly_report.py --lp-or-mpl MPL             # MPL only
"""

import argparse

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import TwoSlopeNorm
import matplotlib.cm as cm

from mix_effects import run_mix_query, METRIC_LABELS

# The metrics shown in the slide, in column order
REPORT_METRICS = [
    ("apr",                         "APR"),
    ("annual_target_return_rate",   "TR"),
    ("combined_fee_rate",           "Fee*"),
    ("expected_annualized_loss_rate", "Loss"),
]

# Segment ordering (matches slide)
SEGMENT_ORDER = ["MPL-Tprime", "MPL Full", "MPL Counter", "LP-Tprime", "LP Other"]
SEGMENT_GROUP = {
    "MPL-Tprime": "MPL",
    "MPL Full":   "MPL",
    "MPL Counter": "MPL",
    "LP-Tprime":  "LP",
    "LP Other":   "LP",
}


def fetch_all_metrics(
    period: str,
    date_type: str,
    lp_or_mpl: str,
    channel: str,
    profile: str = "DEFAULT",
) -> dict[str, pd.DataFrame]:
    """Run a query per metric, return dict of metric_key -> DataFrame."""
    results = {}
    common = dict(
        dim1="fee_segment",
        dim2="all",
        volume="loan_amount",
        period=period,
        date_type=date_type,
        lp_or_mpl=lp_or_mpl,
        channel=channel,
        investor="all",
        risk_grade="all",
        profile=profile,
    )

    for metric_key, label in REPORT_METRICS:
        print(f"  Fetching {label}...", flush=True)
        df = run_mix_query(metric=metric_key, **common)
        results[metric_key] = df

    return results


def build_combined_table(results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Combine per-metric DataFrames into one wide table matching the slide layout.
    Columns: segment, vol_$, vol_pct, vol_wow_pct, [metric_val, metric_wow_bps] x4
    """
    # Use APR df as base for volume (all queries have same volume since same base table)
    base = results["apr"].copy()
    base = base[["dim1", "volume_last_period", "volume_this_period",
                 "volume_share_last_period", "volume_share_this_period"]].copy()
    base.columns = ["segment", "vol_last", "vol_this", "vol_share_last", "vol_share_this"]

    for metric_key, label in REPORT_METRICS:
        df = results[metric_key][["dim1", "metric_last_period", "metric_this_period"]].copy()
        col = label.replace("*", "")
        df = df.rename(columns={
            "dim1": "segment",
            "metric_last_period": f"{col}_last",
            "metric_this_period": f"{col}_this",
        })
        base = base.merge(df, on="segment", how="left")

    # Add totals row
    total = base.drop(columns="segment").sum(numeric_only=True)
    # For rate metrics, compute weighted average
    total_vol_last = base["vol_last"].sum()
    total_vol_this = base["vol_this"].sum()
    for metric_key, label in REPORT_METRICS:
        col = label.replace("*", "")
        mdf = results[metric_key]
        total[f"{col}_last"] = (mdf["metric_last_period"] * mdf["volume_last_period"]).sum() / mdf["volume_last_period"].sum()
        total[f"{col}_this"] = (mdf["metric_this_period"] * mdf["volume_this_period"]).sum() / mdf["volume_this_period"].sum()

    total["vol_last"] = total_vol_last
    total["vol_this"] = total_vol_this
    total["vol_share_last"] = 1.0
    total["vol_share_this"] = 1.0

    total_row = pd.DataFrame([{"segment": "Total", **total.to_dict()}])

    # Add group subtotals
    rows = []
    for group in ["MPL", "LP"]:
        segs = [s for s in SEGMENT_ORDER if SEGMENT_GROUP.get(s) == group]
        subset = base[base["segment"].isin(segs)]
        if len(subset) == 0:
            continue
        sub_vol_last = subset["vol_last"].sum()
        sub_vol_this = subset["vol_this"].sum()
        subtotal = {"segment": f"{group} Total", "vol_last": sub_vol_last, "vol_this": sub_vol_this,
                    "vol_share_last": subset["vol_share_last"].sum(),
                    "vol_share_this": subset["vol_share_this"].sum()}
        for metric_key, label in REPORT_METRICS:
            col = label.replace("*", "")
            mdf = results[metric_key]
            mdf_sub = mdf[mdf["dim1"].isin(segs)]
            subtotal[f"{col}_last"] = (mdf_sub["metric_last_period"] * mdf_sub["volume_last_period"]).sum() / mdf_sub["volume_last_period"].sum()
            subtotal[f"{col}_this"] = (mdf_sub["metric_this_period"] * mdf_sub["volume_this_period"]).sum() / mdf_sub["volume_this_period"].sum()
        rows.append(subtotal)

    subtotals = pd.DataFrame(rows)

    # Order: segment rows, then inject subtotals after each group
    ordered = []
    prev_group = None
    for seg in SEGMENT_ORDER:
        row = base[base["segment"] == seg]
        if not row.empty:
            group = SEGMENT_GROUP.get(seg)
            if prev_group is not None and group != prev_group:
                # Insert previous group subtotal
                st = subtotals[subtotals["segment"] == f"{prev_group} Total"]
                ordered.append(st)
            ordered.append(row)
            prev_group = group

    if prev_group:
        st = subtotals[subtotals["segment"] == f"{prev_group} Total"]
        ordered.append(st)

    ordered.append(total_row)
    combined = pd.concat(ordered, ignore_index=True)
    return combined


def format_bps(val: float) -> str:
    bps = round(val * 10000)
    return f"{bps:+d}bps"


def format_pct(val: float, decimals: int = 1) -> str:
    return f"{val * 100:.{decimals}f}%"


def format_vol(val: float) -> str:
    if val >= 1e9:
        return f"${val/1e9:.1f}B"
    elif val >= 1e6:
        return f"${val/1e6:.0f}m"
    else:
        return f"${val/1e3:.0f}k"


def print_table(combined: pd.DataFrame, period_name: str) -> None:
    print(f"\nPL Pricing W/W  |  {period_name}\n")
    header = f"{'Segment':<18} {'Vol $':>8} {'% PL':>7} {'WoW%':>7}"
    for _, label in REPORT_METRICS:
        col = label.replace("*", "")
        header += f"  {col:>6}  {'WoW':>7}"
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
            val_str = format_pct(this_val) if pd.notna(this_val) else "  -  "
            wow_str = format_bps(wow) if pd.notna(wow) else "  -  "
            line += f"  {val_str:>7}  {wow_str:>7}"

        if is_total:
            print("-" * (len(header) + 10))
        print(line)

    print()


def generate_narrative(combined: pd.DataFrame, results: dict, period_name: str) -> str:
    """
    Generate a plain-English narrative explaining the main WoW drivers.
    This is what an agent would use to answer "what's driving APR changes?".
    """
    total_row = combined[combined["segment"] == "Total"].iloc[0]

    apr_last = total_row["APR_last"]
    apr_this = total_row["APR_this"]
    apr_wow_bps = round((apr_this - apr_last) * 10000)

    tr_last = total_row.get("TR_last", float("nan"))
    tr_this = total_row.get("TR_this", float("nan"))
    tr_wow_bps = round((tr_this - tr_last) * 10000) if pd.notna(tr_last) else 0

    fee_last = total_row.get("Fee_last", float("nan"))
    fee_this = total_row.get("Fee_this", float("nan"))
    fee_wow_bps = round((fee_this - fee_last) * 10000) if pd.notna(fee_last) else 0

    direction = "up" if apr_wow_bps > 0 else "down"
    abs_bps = abs(apr_wow_bps)

    # Find biggest price driver
    apr_df = results["apr"]
    apr_df = apr_df.copy()
    apr_df["wow_bps"] = (apr_df["metric_this_period"] - apr_df["metric_last_period"]) * 10000
    apr_df["vol_wow_pct"] = (apr_df["volume_this_period"] / apr_df["volume_last_period"] - 1) * 100

    biggest_price_mover = apr_df.loc[apr_df["impact_metric_pc"].abs().idxmax()]
    biggest_vol_mover   = apr_df.loc[apr_df["impact_volume_pc"].abs().idxmax()]

    lines = [
        f"## Weekly Mix Effects Summary | {period_name}",
        "",
        f"### Total APR: {apr_this*100:.2f}% ({direction} {abs_bps}bps WoW)",
        "",
        f"**Total WoW change: {apr_wow_bps:+d}bps**",
        f"- Target Return: {tr_wow_bps:+d}bps",
        f"- Fee: {fee_wow_bps:+d}bps",
        "",
        "### Price Effect (rate changes within segments)",
        f"Largest price mover: **{biggest_price_mover['dim1']}** "
        f"({biggest_price_mover['wow_bps']:+.0f}bps WoW, "
        f"contributing {biggest_price_mover['impact_metric_pc']*10000:+.1f}bps to total)",
        "",
        "### Volume Mix Effect (share shifts between segments)",
        f"Largest mix mover: **{biggest_vol_mover['dim1']}** "
        f"(volume {biggest_vol_mover['vol_wow_pct']:+.1f}% WoW, "
        f"contributing {biggest_vol_mover['impact_volume_pc']*10000:+.1f}bps to total)",
        "",
        "### Segment Detail (APR WoW)",
    ]

    for _, row in apr_df.sort_values("impact_metric_pc", key=abs, ascending=False).iterrows():
        wow = row["wow_bps"]
        imp_p = row["impact_metric_pc"] * 10000
        imp_v = row["impact_volume_pc"] * 10000
        vol_w = row["vol_wow_pct"]
        lines.append(
            f"- **{row['dim1']}**: APR {wow:+.0f}bps | vol {vol_w:+.1f}% | "
            f"price impact {imp_p:+.1f}bps | mix impact {imp_v:+.1f}bps"
        )

    return "\n".join(lines)


def save_chart(combined: pd.DataFrame, period_name: str, output_path: str) -> None:
    """Save a styled table image replicating the PL Pricing W/W slide."""
    matplotlib.rcParams["font.family"] = "sans-serif"

    metric_cols = [(label.replace("*", ""), label) for _, label in REPORT_METRICS]
    n_rows = len(combined)

    fig, ax = plt.subplots(figsize=(16, 0.5 + n_rows * 0.55))
    ax.axis("off")

    # Build cell data
    col_headers = ["Segment", "Vol $", "% of PL", "Vol WoW"] + [
        item for _, label in REPORT_METRICS for item in [label.replace("*",""), "WoW"]
    ]
    cell_data = []
    cell_colors = []
    BG_DEFAULT = "#ffffff"
    BG_SUBTOTAL = "#dce6f1"
    BG_TOTAL    = "#2e75b6"

    def bps_color(val_bps: float) -> str:
        if abs(val_bps) < 1:
            return BG_DEFAULT
        cmap = cm.RdYlGn
        norm = TwoSlopeNorm(vmin=-50, vcenter=0, vmax=50)
        rgba = cmap(norm(val_bps))
        return matplotlib.colors.to_hex(rgba)

    for _, row in combined.iterrows():
        seg = str(row["segment"])
        is_total = seg == "Total"
        is_subtotal = "Total" in seg and not is_total
        vol_wow = (row["vol_this"] / row["vol_last"] - 1) if row["vol_last"] > 0 else 0

        cells = [
            seg,
            format_vol(row["vol_this"]),
            format_pct(row["vol_share_this"]),
            f"{vol_wow*100:+.1f}%",
        ]
        colors = [
            BG_TOTAL if is_total else BG_SUBTOTAL if is_subtotal else BG_DEFAULT,
            BG_TOTAL if is_total else BG_SUBTOTAL if is_subtotal else BG_DEFAULT,
            BG_TOTAL if is_total else BG_SUBTOTAL if is_subtotal else BG_DEFAULT,
            BG_TOTAL if is_total else BG_SUBTOTAL if is_subtotal else BG_DEFAULT,
        ]

        for _, label in REPORT_METRICS:
            col = label.replace("*", "")
            this_val = row.get(f"{col}_this", float("nan"))
            last_val = row.get(f"{col}_last", float("nan"))
            wow_bps = (this_val - last_val) * 10000 if pd.notna(this_val) and pd.notna(last_val) else float("nan")
            cells.append(format_pct(this_val) if pd.notna(this_val) else "-")
            cells.append(f"{wow_bps:+.0f}bps" if pd.notna(wow_bps) else "-")
            colors.append(BG_TOTAL if is_total else BG_SUBTOTAL if is_subtotal else BG_DEFAULT)
            colors.append(BG_TOTAL if is_total else bps_color(wow_bps) if pd.notna(wow_bps) else BG_DEFAULT)

        cell_data.append(cells)
        cell_colors.append(colors)

    col_widths = [0.18, 0.07, 0.07, 0.07] + [0.07, 0.08] * len(REPORT_METRICS)

    table = ax.table(
        cellText=cell_data,
        colLabels=col_headers,
        cellColours=cell_colors,
        cellLoc="center",
        loc="center",
        colWidths=col_widths,
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)

    for (r, c), cell in table.get_celld().items():
        cell.set_edgecolor("#cccccc")
        if r == 0:
            cell.set_facecolor("#1f4e79")
            cell.set_text_props(color="white", fontweight="bold")
        else:
            row_seg = combined.iloc[r - 1]["segment"]
            if row_seg == "Total":
                cell.set_text_props(color="white", fontweight="bold")
            elif "Total" in str(row_seg):
                cell.set_text_props(fontweight="bold")

    fig.suptitle(f"PL Pricing W/W  |  {period_name}", fontsize=13, fontweight="bold", y=0.98)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Chart saved to {output_path}")
    plt.close()


def parse_args():
    p = argparse.ArgumentParser(description="Generate PL Pricing W/W report")
    p.add_argument("--period", default="week",
                   choices=["week", "adhoc", "dayday", "dayweek", "l28"])
    p.add_argument("--date-type", default="origination_date",
                   choices=["origination_date", "pricings_created_at_pt_date", "funding_form_submit_pt_date"])
    p.add_argument("--lp-or-mpl", default="all", choices=["all", "MPL", "LP"])
    p.add_argument("--channel", default="all", choices=["all", "onsite", "partner", "dm"])
    p.add_argument("--profile", default="DEFAULT", help="~/.databrickscfg profile to use")
    p.add_argument("--output", default="pl_pricing_wow.png", help="Chart image output path (default: pl_pricing_wow.png)")
    p.add_argument("--no-narrative", action="store_true", help="Skip narrative generation")
    return p.parse_args()


def main():
    args = parse_args()

    print("Fetching mix effects data for all metrics...")
    results = fetch_all_metrics(
        period=args.period,
        date_type=args.date_type,
        lp_or_mpl=args.lp_or_mpl,
        channel=args.channel,
        profile=args.profile,
    )

    print("Building combined table...")
    combined = build_combined_table(results)

    # Get period name from any result
    period_name = results["apr"]["period_name"].iloc[0] if len(results["apr"]) else "unknown"

    print_table(combined, period_name)

    if not args.no_narrative:
        narrative = generate_narrative(combined, results, period_name)
        print(narrative)

    save_chart(combined, period_name, args.output)


if __name__ == "__main__":
    main()
