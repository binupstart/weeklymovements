# Weekly Movements Agent Skill

You are equipped to analyze weekly pricing trends for Upstart's personal loan business.
Use the tools in this repo to fetch live data from Databricks, then synthesize findings
into clear narratives like: *"Total APR is down 12bps due to primer MPL both within
MPL segments and due to a shift from LP."*

---

## What This Does

The **mix effects framework** decomposes week-over-week (WoW) changes in any rate metric
(APR, Fee, Target Return, Loss Rate) into two components:

- **Price Effect** (`impact_metric_pc`): change due to rates moving *within* a segment
  (e.g., MPL Full's APR fell 2bps and it has 39% volume share → −0.8bps impact)
- **Volume Mix Effect** (`impact_volume_pc`): change due to the *mix of business* shifting
  between segments (e.g., MPL Tprime grew from 8%→10% share, and it's lower-APR → −0.4bps)

The two effects sum to approximately the total WoW change in the metric.

---

## Available Scripts

### `mix_effects.py` — Single metric, flexible dimensions

Run one metric with any two breakdown dimensions:

```bash
python mix_effects.py \
  --metric apr \
  --dim1 fee_segment \
  --dim2 risk_grade \
  --period week
```

**Key parameters:**
| Param | Options | Description |
|-------|---------|-------------|
| `--metric` | `apr`, `combined_fee_rate`, `annual_target_return_rate`, `expected_annualized_loss_rate`, `average_loan_size`, `e_share`, `d_per_ffs`, `is_counter_offer_flow` | The rate metric to analyze |
| `--dim1` | `fee_segment`, `investor`, `risk_grade`, `channel`, `lp_or_mpl`, `model`, `fico`, `is_counter`, `is_tprime_borrower`, `all` | Primary breakdown dimension |
| `--dim2` | same as dim1 | Cross-tab dimension (use `all` for single-dimension) |
| `--period` | `week` **(default)**, `l28`, `dayday`, `dayweek` | Comparison window. `week` = last 7 calendar days vs prior 7, relative to last full origination day |
| `--date-type` | `origination_date` **(default)**, `pricings_created_at_pt_date`, `funding_form_submit_pt_date` | Date field. Default to origination_date unless asked about pricing/FFS |
| `--lp-or-mpl` | `all`, `MPL`, `LP` | Filter to one investor type |
| `--channel` | `all`, `onsite`, `partner`, `dm` | Filter to one channel |

**Output columns:**
- `dim1`, `dim2`: segment labels
- `metric_last_period`, `metric_this_period`: rate values (as decimals, multiply by 100 for %)
- `volume_share_last_period`, `volume_share_this_period`: share of total volume
- `impact_metric_pc`: price effect contribution to total metric change
- `impact_volume_pc`: volume mix effect contribution to total metric change

### `weekly_report.py` — Full PL Pricing W/W slide

Runs APR + TR + Fee + Loss simultaneously, by fee_segment, and produces:
1. A formatted terminal table (Vol $, % of PL, WoW changes in bps per metric)
2. A plain-English narrative
3. Optionally a PNG chart (`--output report.png`)

```bash
python weekly_report.py --period week --output weekly_pricing.png
```

---

## Environment Variables Required

```bash
export DATABRICKS_HOST="your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapiXXXXXXXXXXXXXXXX"
export DATABRICKS_WAREHOUSE_ID="xxxxxxxxxxxx"
```

---

## Defaults & Conventions

**When no period or date type is specified, assume:**
- `--period week` — last 7 calendar days vs prior 7 calendar days
- `--date-type origination_date` — we care about when loans originated, not when they were priced

**How "week" works:**
The query looks at the most recent full origination day in the dataset and works backwards.
"This period" = the 7 calendar days ending on that day. "Last period" = the 7 days before that.
Days with no originations (weekends, holidays) are simply absent from the data — that's expected
and the comparison is still valid since it's calendar-day windows, not business-day windows.
This logic is encoded in `dbt_bmalthus.is_last_period()` and `dbt_bmalthus.is_this_period()`.

---

## Dimension Reference

Every `mix_effects.py` run takes `--dim1` and `--dim2`. Here is what each dimension tells you:

| Dimension | What it reveals |
|-----------|----------------|
| `fee_segment` | MPL-Tprime / MPL Full / MPL Counter / LP-Tprime / LP Other — the primary business segmentation. Always start here. |
| `risk_grade` | A/B/C/D/E — credit quality. Drives loss rates, APR, TR. |
| `investor` | Which MPL investor (Core, Fortress, Tprime, Blue Owl, Castlelake, NB). Use with `--lp-or-mpl MPL`. |
| `lp_or_mpl` | MPL vs LP top-level split. Quick sanity check. |
| `channel` | Onsite / Partner / DM. DM loans tend to be larger; partner mix shifts with marketing spend. |
| `is_counter` | Counter-offer flow vs full offer. Counter loans have higher APR/loss. |
| `is_tprime_borrower` | Tprime flag — good proxy for prime vs non-prime within any segment. |
| `model` | Underwriting model version. Useful when a model rollout happened. |
| `fico` | FICO score buckets. More granular than risk_grade for credit quality story. |

**Filter flags** (narrow the population before slicing):
- `--lp-or-mpl MPL` or `LP` — isolate one book
- `--channel dm` / `onsite` / `partner` — isolate one channel
- `--risk-grade A` through `E` — isolate one grade
- `--investor "MPL Core"` etc. — isolate one investor

---

## Investigation Protocol

Always follow this sequence. Run all standard cuts before drawing conclusions.
Stop digging when the story is clear — don't run every possible cut.

### Step 1 — Always run: Full report (fee_segment × all, 4 metrics)

```bash
python weekly_report.py
```

This generates `pl_pricing_wow.png` and prints the table. Read off:
- Total WoW change in APR, TR, Fee, Loss
- Which fee_segments moved in volume and rate
- Whether price or mix effect dominates (from the narrative output)

### Step 2 — Always run: fee_segment × risk_grade for the primary metric

```bash
python mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade
```

This is the most important drill cut. It shows whether movements within a fee_segment
are driven by grade mix shifts (volume effect within segment) or actual repricing
(price effect within grade). Run this for whichever metric you are investigating.

### Step 3 — Run standard high-level cuts for the metric under investigation

Pick from the table below based on what metric you are analyzing:

| Metric | Standard cuts to always run |
|--------|----------------------------|
| `apr` | `fee_segment × risk_grade`, `lp_or_mpl × all`, `investor × all --lp-or-mpl MPL` |
| `combined_fee_rate` (Fee) | `fee_segment × risk_grade`, `investor × all --lp-or-mpl MPL`, `fee_segment × is_counter` |
| `annual_target_return_rate` (TR) | `fee_segment × risk_grade`, `investor × all --lp-or-mpl MPL` |
| `expected_annualized_loss_rate` (Loss) | `risk_grade × all`, `fee_segment × risk_grade`, `fee_segment × is_tprime_borrower` |
| `average_loan_size` | `fee_segment × all`, `risk_grade × all`, `channel × all` |
| `e_share` | `fee_segment × all`, `channel × all` |
| `is_counter_offer_flow` (Counter%) | `channel × all`, `risk_grade × all`, `fee_segment × all` |
| `d_per_ffs` ($/FFS) | `channel × all`, `fee_segment × all`, `is_tprime_borrower × all` |

### Step 4 — Conditional drills based on what you find

**If price effect is the dominant driver:**
- Find which segment(s) had the largest `impact_metric_pc`
- Drill that segment by risk grade: `--dim1 risk_grade --lp-or-mpl MPL` (or LP)
- If MPL: check investor mix: `--dim1 investor --lp-or-mpl MPL`
- If LP: check `--dim1 fee_segment --lp-or-mpl LP --dim2 risk_grade`
- Check `is_tprime_borrower` within the moving segment to see if prime/non-prime mix changed

**If volume mix effect is the dominant driver:**
- Find which segment grew/shrank most in share
- Check if it's grade-driven: `--dim1 risk_grade × all`
- Check if it's channel-driven: `--dim1 channel × all`
- Check EOM patterns: LP throttles EOM, MPL picks up overflow — common on last days of month
- Check counter-offer share: `--dim1 is_counter × all`

**If MPL segments are moving:**
```bash
python mix_effects.py --metric apr --dim1 investor --lp-or-mpl MPL
python mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade --lp-or-mpl MPL
```

**If LP segments are moving:**
```bash
python mix_effects.py --metric apr --dim1 fee_segment --lp-or-mpl LP
python mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade --lp-or-mpl LP
```

**If risk grade mix is shifting (E share growing, grade mix moving):**
```bash
python mix_effects.py --metric apr --dim1 risk_grade --dim2 channel
python mix_effects.py --metric e_share --dim1 fee_segment
python mix_effects.py --metric apr --dim1 risk_grade --dim2 is_tprime_borrower
```

**If you suspect a model change:**
```bash
python mix_effects.py --metric apr --dim1 model --dim2 risk_grade
```

**If the move looks channel-specific:**
```bash
python mix_effects.py --metric apr --dim1 channel --dim2 fee_segment
python mix_effects.py --metric apr --dim1 fee_segment --channel dm
python mix_effects.py --metric apr --dim1 fee_segment --channel onsite
```

### Step 5 — Synthesize the narrative

Structure the answer as:
1. **Headline**: "Total [metric] is [up/down] [X]bps WoW ([this]% vs [last]%)"
2. **Dominant effect**: "This is primarily a [price/mix] effect"
3. **Main driver**: "Driven by [segment] — [what happened and why]"
4. **Supporting colour**: secondary movers, partial offsets
5. **Caveats**: EOM effects, small segments, anything unreliable

Example:
> "Total APR is down 5bps WoW (22.9% vs 23.0%). This is primarily a volume mix story —
> LP-Tprime grade A grew from X% to Y% of total volume, contributing −34.5bps through mix.
> Partially offsetting: MPL Full grade B repriced +10.6bps within segment (+3.7bps price impact).
> MPL Counter rates fell across grades B and D but with limited volume impact."

---

## Key Business Context

**Segments (fee_segment):**
- `MPL-Tprime`: Lowest APR (~13%), primer borrowers, lowest loss. Share growth pulls total APR down.
- `MPL Full`: Largest segment (~38% of vol). Standard MPL full-offer.
- `MPL Counter`: Highest APR (~34%), highest loss. Counter-offer flow.
- `LP-Tprime`: Low APR (~9%), prime LP borrowers on T1/T2 strategy.
- `LP Other`: Mid-APR LP loans (~16%).

**Common patterns to watch for:**
- **EOM LP throttle**: LP reduces volume end-of-month → MPL picks up prime overflow → MPL looks primer, APR drops through mix
- **TR leads Fee**: Target return changes often precede fee adjustments by a few days
- **Counter% spike**: Usually follows a model change or underwriting expansion
- **E share growth**: Watch for risk drift, often channel or model related
- **LP-Tprime A dominating mix**: When prime grade A LP grows, it strongly pulls total APR down (low APR, large volume)

**Units:** All metrics stored as decimals. Multiply × 100 for %, × 10,000 for bps.

**Volume note:** `impact_metric_pc` and `impact_volume_pc` should approximately sum to the
total WoW change. Large residuals indicate non-linearities (e.g., a segment was zero in one period).
