#!/usr/bin/env python3
"""
Demo of yoy_funnel chart output using mock data approximated from the Mode screenshots.
Run without any Databricks connection:
    python3 demo_chart.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from yoy_funnel import draw_chart, format_weighted_table, generate_narrative, FACTOR_KEYS

# Mock data: 8 weeks ending ~01/14, log growth values (ln-scale)
# Approximated from the Mode "Recent Decomp - Log(Growth)" screenshots

# 8 weeks of Mondays ending at the latest full week given 2-day censoring on 03/06/2026.
# max_date = 03/04, so relative_period=0 starts ~01/27 (week of 01/28-02/03... actually
# relative_period 0 = week ending 03/04, working back 7 weeks gives week starting ~01/13).
# Approximated as consecutive Mondays:
WEEKS = ["01/13", "01/20", "01/27", "02/03", "02/10", "02/17", "02/24", "03/03"]

# Log factors per channel per week (values in natural-log %, so +0.20 = +20% ln growth)
MOCK = {
    "Direct Mail": [
        {"f1_ffs": 1.05, "f2_mix": -0.30, "f3_conv": -0.80, "f4_rpl": 0.04},
        {"f1_ffs": 0.95, "f2_mix": -0.28, "f3_conv": -0.72, "f4_rpl": 0.04},
        {"f1_ffs": 0.90, "f2_mix": -0.25, "f3_conv": -0.70, "f4_rpl": 0.03},
        {"f1_ffs": 0.88, "f2_mix": -0.22, "f3_conv": -0.68, "f4_rpl": 0.03},
        {"f1_ffs": 0.85, "f2_mix": -0.20, "f3_conv": -0.50, "f4_rpl": 0.03},
        {"f1_ffs": 1.20, "f2_mix": -0.45, "f3_conv": -0.90, "f4_rpl": 0.04},  # 12/31
        {"f1_ffs": 0.83, "f2_mix": -0.30, "f3_conv": -0.50, "f4_rpl": 0.03},  # 01/07
        {"f1_ffs": 0.85, "f2_mix": -0.25, "f3_conv": -0.40, "f4_rpl": 0.03},  # 01/14
    ],
    "Onsite": [
        {"f1_ffs": 0.35, "f2_mix": -0.10, "f3_conv": -0.15, "f4_rpl": 0.08},
        {"f1_ffs": 0.38, "f2_mix": -0.08, "f3_conv": -0.12, "f4_rpl": 0.09},
        {"f1_ffs": 0.40, "f2_mix": -0.06, "f3_conv": -0.10, "f4_rpl": 0.10},
        {"f1_ffs": 0.42, "f2_mix": -0.06, "f3_conv": -0.09, "f4_rpl": 0.11},
        {"f1_ffs": 0.44, "f2_mix": -0.05, "f3_conv": -0.08, "f4_rpl": 0.12},
        {"f1_ffs": 0.44, "f2_mix": -0.08, "f3_conv": -0.15, "f4_rpl": 0.13},  # 12/31
        {"f1_ffs": 0.44, "f2_mix": -0.05, "f3_conv": -0.10, "f4_rpl": 0.14},  # 01/07
        {"f1_ffs": 0.44, "f2_mix": -0.05, "f3_conv": -0.09, "f4_rpl": 0.14},  # 01/14
    ],
    "Partner": [
        {"f1_ffs": 0.10, "f2_mix": -0.18, "f3_conv": 0.05, "f4_rpl": 0.06},
        {"f1_ffs": 0.15, "f2_mix": -0.15, "f3_conv": 0.07, "f4_rpl": 0.06},
        {"f1_ffs": 0.20, "f2_mix": -0.12, "f3_conv": 0.08, "f4_rpl": 0.07},
        {"f1_ffs": 0.25, "f2_mix": -0.10, "f3_conv": 0.08, "f4_rpl": 0.07},
        {"f1_ffs": 0.28, "f2_mix": -0.08, "f3_conv": 0.07, "f4_rpl": 0.08},
        {"f1_ffs": 0.05, "f2_mix": -0.20, "f3_conv": 0.06, "f4_rpl": 0.06},  # 12/31
        {"f1_ffs": 0.28, "f2_mix": -0.12, "f3_conv": 0.08, "f4_rpl": 0.07},  # 01/07
        {"f1_ffs": 0.37, "f2_mix": -0.07, "f3_conv": 0.07, "f4_rpl": 0.08},  # 01/14
    ],
    "Total": [
        {"f1_ffs": 0.37, "f2_mix": -0.14, "f3_conv": -0.10, "f4_rpl": 0.10},
        {"f1_ffs": 0.38, "f2_mix": -0.12, "f3_conv": -0.08, "f4_rpl": 0.10},
        {"f1_ffs": 0.39, "f2_mix": -0.10, "f3_conv": -0.07, "f4_rpl": 0.11},
        {"f1_ffs": 0.41, "f2_mix": -0.09, "f3_conv": -0.07, "f4_rpl": 0.11},
        {"f1_ffs": 0.42, "f2_mix": -0.08, "f3_conv": -0.06, "f4_rpl": 0.12},
        {"f1_ffs": 0.37, "f2_mix": -0.12, "f3_conv": -0.09, "f4_rpl": 0.10},  # 12/31
        {"f1_ffs": 0.38, "f2_mix": -0.10, "f3_conv": -0.07, "f4_rpl": 0.12},  # 01/07
        {"f1_ffs": 0.37, "f2_mix": -0.10, "f3_conv": -0.10, "f4_rpl": 0.47},  # 01/14
    ],
}

# Add computed totals and week labels
for ch, weeks in MOCK.items():
    for i, w in enumerate(weeks):
        w["label"] = WEEKS[i]
        w["total"] = sum(w.get(k, 0) for k in FACTOR_KEYS)


if __name__ == "__main__":
    channels = ["Direct Mail", "Onsite", "Partner"]

    # Build the decomp dicts in the format generate_narrative expects
    periods = list(range(-7, 1))  # -7 to 0
    plabels = {rp: WEEKS[rp + 7] for rp in periods}

    all_decomps   = {}
    total_decomps = {}
    for i, rp in enumerate(periods):
        for ch in channels:
            d = {k: MOCK[ch][i].get(k) for k in FACTOR_KEYS}
            d["total"] = sum(d[k] for k in FACTOR_KEYS)
            all_decomps[(ch, rp)] = d
        td = {k: MOCK["Total"][i].get(k) for k in FACTOR_KEYS}
        td["total"] = sum(td[k] for k in FACTOR_KEYS)
        total_decomps[rp] = td

    print("─── Log(Growth) Charts — Independent Scale ───────────────────────────────────")
    for ch in channels:
        draw_chart(MOCK[ch], f"{ch} (within-channel mix)")
    draw_chart(MOCK["Total"], "Total (global mix — includes cross-channel TOFU mix)")

    generate_narrative(all_decomps, total_decomps, channels, periods, plabels)
