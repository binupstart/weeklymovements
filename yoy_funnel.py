#!/usr/bin/env python3
"""
Y/Y growth funnel decomposition for personal loans — 8-week trending view.

Decomposes Y/Y revenue growth into 4 additive log factors per channel per week:
  (1) FFS volume     ln(ffs_tp / ffs_lp)
  (2) TOFU mix       ln(rev_tp / rev_mix)          within-channel for split; global for Total
  (3) Conv / FFS     ln((orig_mix/ffs_mix) / (orig_lp/ffs_lp))
  (4) Rev / loan     ln((rev_mix/orig_mix)  / (rev_lp/orig_lp))

Sum of 4 factors = ln(rev_tp / rev_lp)  [exact identity]

Output:
  - ASCII stacked bar chart per channel (independent log scale, like Mode chart 3)
  - Weighted contribution table (channels scaled to total revenue base, like Mode chart 4)
  - Narrative summary of recent trends

Run directly:
    python3 yoy_funnel.py
    python3 yoy_funnel.py --num-weeks 12
    python3 yoy_funnel.py --censoring unrestricted
    python3 yoy_funnel.py --channel Onsite Partner
"""

import argparse
import math
import sys
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, str(Path(__file__).parent))
from mix_effects import get_databricks_config, run_query

SCRIPT_DIR = Path(__file__).parent
WAREHOUSE_ID = "ef12a30cfaa1ef9b"

CHANNEL_ALL = ("Onsite", "Partner", "Direct Mail")
CHANNEL_ORDER = ["Direct Mail", "Onsite", "Partner"]

# Maps granular channel names (from SQL channel_summary) → rollup names
CHANNEL_ROLLUP_MAP = {
    "Digital": "Onsite",
    "Organic": "Onsite",
    "SEO": "Onsite",
    "Email": "Onsite",
    "API": "Partner",
    "ITA": "Partner",
    "Lightbox": "Partner",
    "Direct Mail": "Direct Mail",
}


# ── SQL ───────────────────────────────────────────────────────────────────────

def render_sql(censoring, channel_sql, relative_period=None, min_relative_period=None) -> str:
    env = Environment(loader=FileSystemLoader(str(SCRIPT_DIR)))
    t = env.get_template("yoy_funnel.sql.j2")
    return t.render(
        censoring=censoring,
        channel_sql=channel_sql,
        relative_period=relative_period,
        min_relative_period=min_relative_period,
    )


def run_funnel_query(
    censoring="2day",
    channels=None,
    num_weeks=8,
    profile="DEFAULT",
) -> pd.DataFrame:
    if channels is None or "all" in channels:
        channel_sql = ", ".join(f"'{c}'" for c in CHANNEL_ALL)
    else:
        channel_sql = ", ".join(f"'{c}'" for c in channels)

    sql = render_sql(censoring, channel_sql, min_relative_period=-(num_weeks - 1))
    host, token = get_databricks_config(profile)
    df = run_query(sql, WAREHOUSE_ID, host, token)

    non_numeric = {"channel_type", "channel_summary", "fico", "channel", "start_date"}
    for col in df.columns:
        if col not in non_numeric:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add rollup channel column for aggregating granular channels → Onsite/Partner/Direct Mail
    df["channel_rollup"] = df["channel_summary"].map(CHANNEL_ROLLUP_MAP)

    return df


# ── Aggregation ───────────────────────────────────────────────────────────────

METRIC_COLS = [
    "ffs_count", "ffs_count_mix", "ffs_count_lp",
    "got_rate_count", "got_rate_count_mix", "got_rate_count_lp",
    "origination_count", "origination_count_mix", "origination_count_lp",
    "revenue_value", "revenue_value_mix", "revenue_value_lp",
]


def aggregate(df, channel_type, channel_summary, relative_period):
    if channel_type == "split":
        # Match by rollup (sums across granular channels within the rollup)
        sub = df[
            (df["channel_rollup"] == channel_summary) &
            (df["channel_type"] == channel_type) &
            (df["relative_period"] == relative_period)
        ]
    else:
        sub = df[
            (df["channel_type"] == channel_type) &
            (df["channel_summary"] == channel_summary) &
            (df["relative_period"] == relative_period)
        ]
    if sub.empty:
        return {}
    return {col: sub[col].sum() for col in METRIC_COLS if col in sub.columns}


def get_sorted_periods(df) -> list[int]:
    return sorted(df["relative_period"].dropna().unique().astype(int))


def get_channels(df) -> list[str]:
    present = set(df[df["channel_type"] == "split"]["channel_rollup"].dropna().unique())
    ordered = [c for c in CHANNEL_ORDER if c in present]
    return ordered + sorted(present - set(CHANNEL_ORDER))


def period_label(df, rp: int) -> str:
    sub = df[df["relative_period"] == rp]["start_date"].dropna()
    if sub.empty:
        return str(rp)
    return pd.to_datetime(sub.iloc[0]).strftime("%m/%d")


# ── Decomposition math ────────────────────────────────────────────────────────

def _ln(x):
    return math.log(x) if x and x > 0 else None


def compute_log_decomp(m: dict) -> dict | None:
    ffs_tp   = m.get("ffs_count") or 0
    ffs_lp   = m.get("ffs_count_lp") or 0
    rev_tp   = m.get("revenue_value") or 0
    rev_mix  = m.get("revenue_value_mix") or 0
    rev_lp   = m.get("revenue_value_lp") or 0
    orig_mix = m.get("origination_count_mix") or 0
    orig_lp  = m.get("origination_count_lp") or 0

    if ffs_lp == 0 or rev_lp == 0:
        return None

    f1 = _ln(ffs_tp / ffs_lp)
    f2 = _ln(rev_tp / rev_mix)           if rev_mix  > 0 else None
    f3 = _ln((orig_mix / ffs_tp) / (orig_lp / ffs_lp)) \
         if ffs_tp > 0 and orig_lp > 0 and orig_mix > 0 else None
    f4 = _ln((rev_mix / orig_mix) / (rev_lp / orig_lp)) \
         if orig_mix > 0 and orig_lp > 0 else None

    return {
        "f1_ffs":  f1,
        "f2_mix":  f2,
        "f3_conv": f3,
        "f4_rpl":  f4,
        "total":   _ln(rev_tp / rev_lp),
    }


FACTOR_KEYS  = ["f1_ffs", "f2_mix", "f3_conv", "f4_rpl"]
FACTOR_NAMES = ["FFS vol", "TOFU mix", "Conv/FFS", "Rev/loan"]

# ── ASCII chart ───────────────────────────────────────────────────────────────
# Positive factors: dark block chars stacking upward
# Negative factors: lighter chars stacking downward
# ● marks the actual total ln(Y/Y)

POS_CHARS = {"f1_ffs": "█", "f2_mix": "▓", "f3_conv": "░", "f4_rpl": "▪"}
NEG_CHARS = {"f1_ffs": "▇", "f2_mix": "▒", "f3_conv": "▏", "f4_rpl": "·"}

LEGEND = "█ FFS  ▓ TOFU mix  ░ Conv/FFS  ▪ Rev/loan  (▇▒▏· = negatives)  ● ln(Rev Y/Y)"


def _row_pct(row_idx: int, zero_row: int, row_height: float) -> float:
    """Converts a grid row index to a % value (positive above zero, negative below)."""
    return (zero_row - row_idx) * row_height


def draw_chart(weeks: list[dict], title: str) -> None:
    """
    weeks: list of {'label': 'MM/DD', 'f1_ffs': float, ..., 'total': float}
    Draws a vertical stacked bar chart to stdout.
    """
    if not weeks:
        return

    # Determine y range
    extremes = [0.0]
    for w in weeks:
        pos = sum((w.get(k) or 0) for k in FACTOR_KEYS if (w.get(k) or 0) > 0)
        neg = sum((w.get(k) or 0) for k in FACTOR_KEYS if (w.get(k) or 0) < 0)
        extremes += [pos, neg, w.get("total") or 0]

    y_max_raw = max(extremes) * 100
    y_min_raw = min(extremes) * 100

    # Pick row_height so we get ~16 rows
    span = max(y_max_raw - y_min_raw, 10)
    for candidate in [5, 10, 15, 20, 25, 30]:
        if span / candidate <= 20:
            row_height = candidate
            break
    else:
        row_height = math.ceil(span / 16 / 5) * 5

    n_pos_rows = math.ceil(max(y_max_raw, 0) / row_height)
    n_neg_rows = math.ceil(max(-y_min_raw, 0) / row_height)
    n_rows = n_pos_rows + n_neg_rows
    zero_row = n_pos_rows

    Y_LABEL_W = 7   # "+100% |"
    BAR_W = 6       # 4 chars bar + 2 space (extra space ensures x-axis labels don't run together)
    width = Y_LABEL_W + len(weeks) * BAR_W + 1

    grid = [[" "] * width for _ in range(n_rows + 1)]

    # Y-axis
    for r in range(n_rows + 1):
        pct = _row_pct(r, zero_row, row_height)
        if pct % 20 == 0:
            lbl = f"{pct:+.0f}%".rjust(Y_LABEL_W - 2)
            for i, ch in enumerate(lbl):
                grid[r][i] = ch
        grid[r][Y_LABEL_W - 1] = "┼" if r == zero_row else ("┤" if pct % 20 == 0 else "│")

    # Zero line
    for x in range(Y_LABEL_W, width):
        if grid[zero_row][x] == " ":
            grid[zero_row][x] = "─"

    # Fill bars
    for wi, w in enumerate(weeks):
        x0 = Y_LABEL_W + wi * BAR_W

        # Positive stack upward from zero
        pos_offset = 0
        for k in FACTOR_KEYS:
            v = w.get(k) or 0
            if v > 0:
                n = max(1, round(v * 100 / row_height))
                ch = POS_CHARS[k]
                for r in range(n):
                    row = zero_row - pos_offset - r - 1
                    if 0 <= row < n_rows + 1:
                        grid[row][x0] = ch
                        grid[row][x0 + 1] = ch
                pos_offset += n

        # Negative stack downward from zero
        neg_offset = 0
        for k in FACTOR_KEYS:
            v = w.get(k) or 0
            if v < 0:
                n = max(1, round(-v * 100 / row_height))
                ch = NEG_CHARS[k]
                for r in range(n):
                    row = zero_row + neg_offset + r
                    if 0 <= row < n_rows + 1:
                        grid[row][x0] = ch
                        grid[row][x0 + 1] = ch
                neg_offset += n

        # Revenue total marker ●
        total = w.get("total") or 0
        total_row = zero_row - round(total * 100 / row_height)
        if 0 <= total_row < n_rows + 1:
            grid[total_row][x0 + 3] = "●"

    # Print
    print(f"\n  {title}")
    print(f"  {LEGEND}")
    for row in grid:
        print("  " + "".join(row))

    # X-axis labels (week start dates) — padded to match bar width
    x_labels = " " * Y_LABEL_W + "".join(f"{w['label'][:5]:<{BAR_W}}" for w in weeks)
    print("  " + x_labels)


# ── Weighted contribution table ───────────────────────────────────────────────

def format_weighted_table(
    all_metrics: dict,   # (channel, rp) → metrics dict
    all_decomps: dict,   # (channel, rp) → decomp dict
    total_metrics: dict, # rp → metrics dict
    total_decomps: dict, # rp → decomp dict
    channels: list[str],
    periods: list[int],
    period_labels: dict,
) -> None:
    """
    Weighted contribution view: each channel's log factors × (rev_lp_c / rev_lp_total).
    Columns: weeks. Rows: per-channel per-factor, then Total column for comparison.
    """
    COL = 7

    def _p(v):
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "  n/a"
        return f"{v*100:+.1f}%"

    def header_row(label, vals):
        print(f"  {label:<22}" + "".join(f"{v:>{COL}}" for v in vals))

    print("\n─── Weighted Contribution (channels scaled to total revenue base) " + "─" * 14)
    print("  (per-channel log factors × LY rev weight; gap between ∑channels and Total = cross-channel mix)")

    week_labels = [period_labels[rp] for rp in periods]

    for ch in channels:
        print(f"\n  ── {ch} ──")
        header_row("", week_labels)
        print("  " + "─" * (22 + COL * len(periods)))

        # LY revenue weight per week
        weights = []
        for rp in periods:
            tm = total_metrics.get(rp, {})
            cm = all_metrics.get((ch, rp), {})
            rev_lp_total = tm.get("revenue_value_lp") or 1
            rev_lp_ch = cm.get("revenue_value_lp") or 0
            weights.append(rev_lp_ch / rev_lp_total)
        header_row("LY rev weight", [f"{w*100:.1f}%" for w in weights])
        print()

        for fk, fname in zip(FACTOR_KEYS, FACTOR_NAMES):
            vals = []
            for i, rp in enumerate(periods):
                d = all_decomps.get((ch, rp), {})
                v = d.get(fk)
                wv = (weights[i] * v) if (v is not None and weights[i] is not None) else None
                vals.append(_p(wv))
            header_row(f"  ({FACTOR_KEYS.index(fk)+1}) {fname}", vals)

        # Channel contribution
        contribs = []
        for i, rp in enumerate(periods):
            d = all_decomps.get((ch, rp), {})
            v = d.get("total")
            wv = (weights[i] * v) if (v is not None and weights[i] is not None) else None
            contribs.append(_p(wv))
        print("  " + "─" * (22 + COL * len(periods)))
        header_row("  Contribution", contribs)

    # Total row
    print(f"\n  ── Total (global mix) ──")
    header_row("", week_labels)
    print("  " + "─" * (22 + COL * len(periods)))
    for fk, fname in zip(FACTOR_KEYS, FACTOR_NAMES):
        vals = [_p(total_decomps.get(rp, {}).get(fk)) for rp in periods]
        header_row(f"  ({FACTOR_KEYS.index(fk)+1}) {fname}", vals)
    print("  " + "─" * (22 + COL * len(periods)))
    header_row("  Total ln(Y/Y)", [_p(total_decomps.get(rp, {}).get("total")) for rp in periods])

    # Cross-channel mix gap per week
    gaps = []
    for rp in periods:
        total_log = (total_decomps.get(rp) or {}).get("total")
        ch_sum = 0.0
        valid = True
        tm = total_metrics.get(rp, {})
        rev_lp_total = tm.get("revenue_value_lp") or 1
        for ch in channels:
            d = all_decomps.get((ch, rp), {})
            cm = all_metrics.get((ch, rp), {})
            w = (cm.get("revenue_value_lp") or 0) / rev_lp_total
            v = d.get("total")
            if v is None:
                valid = False
                break
            ch_sum += w * v
        gaps.append(_p(total_log - ch_sum) if (valid and total_log is not None) else "  n/a")

    header_row("  Cross-channel mix gap", gaps)
    print()


# ── Narrative summary ─────────────────────────────────────────────────────────

def _lp(v):
    """Format a log-% value with sign, one decimal."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "  n/a"
    return f"{v*100:+.1f}%"


def _ay(v):
    """Format actual Y/Y growth (exp(ln_v) - 1) as percentage."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "  n/a"
    return f"{(math.exp(v) - 1)*100:+.1f}%"


def _delta(this, prior):
    """Difference in log-% points (this week minus prior week)."""
    if this is None or prior is None:
        return None
    return this - prior


def _consecutive_direction(vals: list) -> tuple[int, str]:
    """
    Returns (streak_length, 'up'|'down'|'flat') for the most recent run
    of the same sign at the end of vals.
    """
    clean = [(i, v) for i, v in enumerate(vals) if v is not None]
    if len(clean) < 2:
        return 0, "flat"
    # Walk backwards from the end
    last_sign = 1 if clean[-1][1] > 0 else -1
    streak = 1
    for i in range(len(clean) - 2, -1, -1):
        s = 1 if clean[i][1] > 0 else -1
        if s == last_sign:
            streak += 1
        else:
            break
    direction = "up" if last_sign > 0 else "down"
    return streak, direction


def generate_narrative(
    all_decomps: dict,   # (channel, rp) → decomp dict
    total_decomps: dict, # rp → decomp dict
    channels: list[str],
    periods: list[int],
    period_labels: dict,
) -> None:
    """
    Summary focused on:
    1. This week's actual Y/Y breakdown (per channel + total)
    2. WoW change in each factor vs. prior week
    3. Notable WoW moves flagged explicitly
    4. 8-week sustained trends (only if signal is clear)
    """
    if len(periods) < 2:
        return

    rp_now  = periods[-1]
    rp_prev = periods[-2]

    lbl_now  = period_labels[rp_now]
    lbl_prev = period_labels[rp_prev]

    ALL_CHANNELS = channels + ["Total"]
    COLS = ["total"] + FACTOR_KEYS
    COL_NAMES = ["ln(RevY/Y)", "FFS vol", "TOFU mix", "Conv/FFS", "Rev/loan"]
    COL_W = 9
    NAME_W = 16

    def get_decomp(ch, rp):
        if ch == "Total":
            return total_decomps.get(rp, {})
        return all_decomps.get((ch, rp), {})

    print("\n─── Summary: This Week vs. Prior Week " + "─" * 43)
    print(f"  This week: {lbl_now}  |  Δ = change vs. prior week ({lbl_prev})")
    print(f"  ln(%) values — 4 factors sum to ln(Rev Y/Y). 'actual Y/Y' = exp(ln) - 1.\n")

    # Header
    hdr = f"  {'':>{NAME_W}}" + "".join(f"{n:>{COL_W}}" for n in COL_NAMES)
    print(hdr)
    divider = "  " + "─" * (NAME_W + COL_W * len(COLS))

    # Collect deltas for later flagging
    notable = []   # (|delta|, label_str)

    for ch in ALL_CHANNELS:
        print(divider)
        d_now  = get_decomp(ch, rp_now)
        d_prev = get_decomp(ch, rp_prev)

        now_vals   = [d_now.get(c)  for c in COLS]
        prev_vals  = [d_prev.get(c) for c in COLS]
        delta_vals = [_delta(n, p) for n, p in zip(now_vals, prev_vals)]

        label = ch if ch == "Total" else ch
        print(f"  {label + ' now':<{NAME_W}}" + "".join(f"{_lp(v):>{COL_W}}" for v in now_vals))
        print(f"  {'  actual Y/Y':<{NAME_W}}{_ay(now_vals[0]):>{COL_W}}")
        print(f"  {'  Δ (ln)':<{NAME_W}}" + "".join(f"{_lp(v):>{COL_W}}" for v in delta_vals))

        # Collect notable deltas (skip Rev Y/Y total delta, focus on factors)
        for i, fk in enumerate(FACTOR_KEYS):
            dv = delta_vals[i + 1]  # +1 because COLS[0] = "total"
            if dv is not None and abs(dv) >= 0.05:  # ≥5pp
                fname = FACTOR_NAMES[i]
                notable.append((abs(dv), dv, ch, fname, d_now.get(fk)))

    print(divider)
    print()

    # Notable WoW changes — sorted largest first
    if notable:
        notable.sort(key=lambda x: x[0], reverse=True)
        print("  Notable WoW changes (|Δ| ≥ 5pp):")
        for _, dv, ch, fname, now_v in notable[:6]:  # cap at 6 bullets
            direction = "improved" if dv > 0 else "widened drag" if (now_v or 0) < 0 else "deteriorated"
            now_str   = _lp(now_v)
            delta_str = _lp(dv)
            print(f"  ● {ch} {fname}: {delta_str} WoW  →  now {now_str}  [{direction}]")
        print()

    # 8-week sustained trends — only flag if streak ≥ 5 weeks
    trends = []
    for ch in ALL_CHANNELS:
        for fk, fname in zip(FACTOR_KEYS, FACTOR_NAMES):
            vals = [get_decomp(ch, rp).get(fk) for rp in periods]
            streak, direction = _consecutive_direction(vals)
            if streak >= 5:
                latest_v = get_decomp(ch, rp_now).get(fk)
                trends.append((streak, ch, fname, direction, latest_v))

    if trends:
        trends.sort(key=lambda x: x[0], reverse=True)
        print("  8-week sustained trends:")
        for streak, ch, fname, direction, latest_v in trends:
            arrow = "↑" if direction == "up" else "↓"
            print(f"  {arrow} {ch} {fname}: {streak} consecutive weeks {direction}  (latest {_lp(latest_v)})")
        print()


# ── Top-level output ──────────────────────────────────────────────────────────

def format_results(df: pd.DataFrame, censoring: str, channel_label: str, num_weeks: int) -> None:
    periods = get_sorted_periods(df)
    # Only current-year periods (≤ 0); positive periods are future-year LY-only rows
    periods = [rp for rp in periods if rp <= 0]
    # Keep only the most recent num_weeks
    periods = periods[-num_weeks:]

    channels = get_channels(df)
    plabels = {rp: period_label(df, rp) for rp in periods}

    # Build aggregated metrics and decomps for all (channel, period) and (total, period)
    all_metrics = {}
    all_decomps = {}
    total_metrics = {}
    total_decomps = {}

    for rp in periods:
        tm = aggregate(df, "total", "Total", rp)
        total_metrics[rp] = tm
        total_decomps[rp] = compute_log_decomp(tm) or {}

        for ch in channels:
            m = aggregate(df, "split", ch, rp)
            all_metrics[(ch, rp)] = m
            all_decomps[(ch, rp)] = compute_log_decomp(m) or {}

    # ── Header ──
    latest_label = plabels.get(periods[-1], "?") if periods else "?"
    print(f"\n=== Y/Y Funnel Decomposition | {num_weeks}-week trend ending {latest_label} ===")
    print(f"Censoring: {censoring}  |  Channels: {channel_label}\n")

    # ── ASCII stacked bar charts ─────────────────────────────────────────────
    print("─── Log(Growth) Charts — Independent Scale " + "─" * 37)

    # One chart per channel (within-channel mix)
    for ch in channels:
        weeks_data = []
        for rp in periods:
            d = all_decomps.get((ch, rp), {})
            if d:
                weeks_data.append({"label": plabels[rp], **d})
        if any(w.get("total") is not None for w in weeks_data):
            draw_chart(weeks_data, f"{ch} (within-channel mix)")

    # Total chart (global mix)
    total_weeks = []
    for rp in periods:
        d = total_decomps.get(rp, {})
        if d:
            total_weeks.append({"label": plabels[rp], **d})
    if any(w.get("total") is not None for w in total_weeks):
        draw_chart(total_weeks, "Total (global mix — includes cross-channel TOFU mix)")

    # ── Weighted contribution table ──────────────────────────────────────────
    format_weighted_table(
        all_metrics, all_decomps,
        total_metrics, total_decomps,
        channels, periods, plabels,
    )

    # ── Narrative ────────────────────────────────────────────────────────────
    generate_narrative(all_decomps, total_decomps, channels, periods, plabels)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Run Y/Y funnel decomposition on Databricks")
    p.add_argument(
        "--censoring",
        default="2day",
        choices=["unrestricted", "0day", "1day", "2day"],
        help=(
            "Accept lag window applied uniformly across history for comparable trends. "
            "'2day' (default): only loans accepted within 2 days of FFS. "
            "'unrestricted': all acceptances ever."
        ),
    )
    p.add_argument(
        "--channel",
        nargs="+",
        default=["all"],
        metavar="CHANNEL",
        help="Onsite, Partner, 'Direct Mail', or all (default: all)",
    )
    p.add_argument(
        "--num-weeks",
        type=int,
        default=8,
        help="Number of recent weeks to show (default: 8)",
    )
    p.add_argument("--profile", default="DEFAULT", help="~/.databrickscfg profile")
    p.add_argument("--print-sql", action="store_true", help="Print rendered SQL and exit")
    p.add_argument("--output", help="Save raw query results to CSV")
    return p.parse_args()


def main():
    args = parse_args()

    if "all" in args.channel:
        channels = ["all"]
        channel_label = "All"
    else:
        channels = args.channel
        channel_label = " + ".join(args.channel)

    if args.print_sql:
        cs = ", ".join(f"'{c}'" for c in (CHANNEL_ALL if "all" in channels else channels))
        print(render_sql(args.censoring, cs, min_relative_period=-(args.num_weeks - 1)))
        return

    print(
        f"Running Y/Y funnel: channel={channel_label}, "
        f"censoring={args.censoring}, last {args.num_weeks} weeks..."
    )
    df = run_funnel_query(
        censoring=args.censoring,
        channels=channels,
        num_weeks=args.num_weeks,
        profile=args.profile,
    )

    if args.output:
        df.to_csv(args.output, index=False)
        print(f"Saved to {args.output}")
        return

    if df.empty:
        print("No data returned.")
        return

    format_results(df, args.censoring, channel_label, args.num_weeks)


if __name__ == "__main__":
    main()
