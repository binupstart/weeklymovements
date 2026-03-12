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

### `yoy_funnel.py` — Year-over-year funnel decomposition (8-week trending view)

Decomposes Y/Y revenue growth into 4 additive log factors, showing the last 8 weeks per channel.

```bash
python3 yoy_funnel.py                          # default: 8 weeks, all channels, 2-day censoring
python3 yoy_funnel.py --num-weeks 12           # show more history
python3 yoy_funnel.py --censoring unrestricted # include all late acceptances
python3 yoy_funnel.py --channel Onsite Partner # filter to specific channels
python3 yoy_funnel.py --print-sql              # inspect rendered SQL before running
```

**Key parameters:**
| Param | Default | Description |
|-------|---------|-------------|
| `--censoring` | `2day` | Accept lag window applied uniformly across history so trends are comparable. `2day` = only loans accepted within 2 days of FFS (standard). `unrestricted` = all acceptances ever. |
| `--channel` | all | Filter to: `Onsite`, `Partner`, `'Direct Mail'` (space-separated, or omit for all) |
| `--num-weeks` | 8 | How many recent weeks to fetch and chart |

**The 4 decomposition factors (additive in log-space; sum = ln(rev Y/Y) exactly):**
1. **FFS volume** — raw Y/Y growth in funding form submits: `ln(ffs_tp / ffs_lp)`
2. **TOFU mix** — how the FICO × loan-size distribution changed vs. last year within each channel: `ln(rev_tp / rev_mix)`. Positive = better applicant mix than LY; negative = worse.
3. **Conv / FFS** — change in origination rate holding LY applicant mix constant: `ln((orig_mix/ffs_mix) / (orig_lp/ffs_lp))`
4. **Rev / loan** — change in fee per origination holding LY mix constant: `ln((rev_mix/orig_mix) / (rev_lp/orig_lp))`

**Mix adjustment design (important to understand):**
The query runs two parallel mix adjustments:
- **Global mix** (`mix_adjusted` CTE, `channel_type='total'`): reweights cells to LY's FICO × channel × loan-size distribution across the entire book. The Total chart's TOFU mix factor captures both cross-channel shifts (e.g. DM growing vs Partner) and within-channel composition changes.
- **Within-channel mix** (`mix_adjusted_channel` CTE, `channel_type='split'`): reweights cells to LY's FICO × loan-size distribution *within each channel separately*. Per-channel TOFU mix factors only capture intra-channel composition changes — DM growing vs Partner shows up in the Total's TOFU mix gap, not in DM's own TOFU mix.

This means: if DM grows a lot (which is negative for total revenue quality since DM/FFS is low), that cross-channel shift is visible in the Total TOFU mix but is not penalised in DM's own chart. That's intentional — within DM, growth is growth.

**Three output sections:**
1. **ASCII stacked bar charts** (8 weeks, one per channel + Total): factors stack to the `●` revenue Y/Y marker. Positive factors use dark chars (█▓░▪), negative use light chars (▇▒▏·).
2. **Weighted contribution table**: each channel's factors × (LY rev share), so channels sum to Total. Gap between ∑channels and Total = cross-channel TOFU mix shift.
3. **Summary: This Week vs. Prior Week**:
   - Table of this week's actual Y/Y factors + Δ (WoW change in each factor) per channel
   - Notable WoW changes bulleted, sorted by magnitude (≥5pp flagged)
   - 8-week sustained trends flagged only if ≥5 consecutive weeks in same direction

**Funnel stages tracked:** FFS → Got Rate (approvals) → Originations → Revenue (origination fee)

**When to use this vs. `mix_effects.py`:**
- `mix_effects.py` → WoW analysis of rate metrics (APR, Fee, TR, Loss) with flexible dimension cuts
- `yoy_funnel.py` → Y/Y growth analysis showing whether growth is driven by volume, applicant quality, conversion, or fee rate, trended over 8 weeks by channel

---

### `mix_effects.py` — Single metric, flexible dimensions

Run one metric with any two breakdown dimensions:

```bash
python3 mix_effects.py \
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

---

## Auth & Config

Auth is read automatically from `~/.databrickscfg` (Databricks CLI config). No credentials
needed in code or environment. Warehouse ID is hardcoded in `mix_effects.py`.

To use a non-default profile: `--profile my_profile_name`

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

### Step 1 — Always run: fee_segment × all for the 4 core metrics

```bash
python3 mix_effects.py --metric apr --dim1 fee_segment
python3 mix_effects.py --metric annual_target_return_rate --dim1 fee_segment
python3 mix_effects.py --metric combined_fee_rate --dim1 fee_segment
python3 mix_effects.py --metric expected_annualized_loss_rate --dim1 fee_segment
```

Read off:
- Total WoW change in APR, TR, Fee, Loss
- Which fee_segments moved in volume and rate
- Whether price or mix effect dominates (`impact_metric_pc` vs `impact_volume_pc`)
- Whether TR and Fee are moving together or diverging (if opposite directions, flag it)

### Step 2 — Always run: fee_segment × risk_grade for the primary metric

```bash
python3 mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade
```

This is the most important drill cut. It shows whether movements within a fee_segment
are driven by grade mix shifts (volume effect within segment) or actual repricing
(price effect within grade). Run this for whichever metric you are investigating.

### Step 3 — Run standard high-level cuts for the metric under investigation

Pick from the table below based on what metric you are analyzing:

| Metric | Standard cuts to always run |
|--------|----------------------------|
| `apr` | `fee_segment × risk_grade`, `lp_or_mpl × all`, `investor × all --lp-or-mpl MPL`, `channel × fee_segment`, `fico --channel dm --lp-or-mpl MPL`, `fico --channel onsite --lp-or-mpl MPL` |

For APR, always run the channel FICO cuts:

```bash
python3 mix_effects.py --metric apr --dim1 channel --dim2 fee_segment
python3 mix_effects.py --metric apr --dim1 fico --channel dm --lp-or-mpl MPL
python3 mix_effects.py --metric apr --dim1 fico --channel onsite --lp-or-mpl MPL
```

DM frequently brings a lower-FICO borrower mix which drives more counter-offer flow (MPL Counter) and higher average APR through credit quality mix, not repricing. Onsite tends to show cleaner within-cell rate signals. Comparing the two FICO distributions separates genuine rate changes from channel-driven credit quality shifts.
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
- Drill that segment by risk grade: `--dim1 fee_segment --dim2 risk_grade`
- **Do not conclude repricing until you've run `investor × risk_grade` and `fico × all`.**
  A large price effect at a coarse level (investor, fee_segment) is often grade mix at
  a finer level. True repricing shows up as large within-cell changes when you control for grade.
- If MPL: run the full MPL deep-dive sequence (see below)
- If LP: check `--dim1 fee_segment --lp-or-mpl LP --dim2 risk_grade`
- Check `is_tprime_borrower` within the moving segment to see if prime/non-prime mix changed

**If volume mix effect is the dominant driver:**
- Find which segment grew/shrank most in share
- Check if it's grade-driven: `--dim1 risk_grade × all`
- Check if it's channel-driven: `--dim1 channel × all`
- Check EOM patterns: LP throttles EOM, MPL picks up overflow — common on last days of month
- Check counter-offer share: `--dim1 is_counter × all`

**If MPL segments are moving — run the full MPL deep-dive sequence:**

MPL investor-level moves are often grade mix masquerading as repricing. Always run all
three of these before concluding an investor repriced:

```bash
# 1. Investor × all: which investors moved? (coarse — grade mix confounds this)
python3 mix_effects.py --metric apr --dim1 investor --lp-or-mpl MPL

# 2. Investor × risk_grade: controls for grade mix within each investor.
#    If within-cell changes are small here, investor×all "repricing" was grade mix, not real.
#    If within-cell changes are large (>20bps), that investor genuinely repriced.
python3 mix_effects.py --metric apr --dim1 investor --dim2 risk_grade --lp-or-mpl MPL

# 3. FICO × all: most granular credit quality cut.
#    Large price effects here = real rate movement. FICO mix shift = composition, not repricing.
#    FICO bands: [0-640), [640-660), [660-680), [680-700), [700-720), [720+)
python3 mix_effects.py --metric apr --dim1 fico --lp-or-mpl MPL
```

**How to read investor × risk_grade:**
- **Small within-cell changes (<10bps) + large mix impacts** → investor-level move was grade
  composition. The investor didn't reprice; it just originated a different grade mix this week.
- **Large within-cell changes (>20bps) consistent across grades** → genuine repricing for
  that investor. All grades moved together = rate sheet change.
- **Large within-cell changes for only one or two grades** → selective grade-level adjustment
  or data artefact (small volume in that cell — check `volume_last_period` / `volume_this_period`).

**How to read FICO:**
- FICO is a proxy for risk, which determines grade, which determines pricing.
- If FICO mix shifted (large `impact_volume_pc` at the FICO level) → underlying credit quality
  of originations changed. This will flow through to grade mix changes next week.
- If FICO price effects are large in a specific band → real rate changes targeted at that
  credit tier, likely a deliberate pricing or model adjustment.
- A move in FICO 680–720 is the most impactful band (highest volume, mid-range APR).

**If LP segments are moving — LP deep-dive sequence:**

LP is simpler than MPL (no investor dimension) but grade mix is still the primary confound:

```bash
# 1. Top-level LP split
python3 mix_effects.py --metric apr --dim1 fee_segment --lp-or-mpl LP

# 2. Grade mix within LP segments — always run before calling it repricing
python3 mix_effects.py --metric apr --dim1 fee_segment --dim2 risk_grade --lp-or-mpl LP

# 3. Is_tprime_borrower — quick prime/non-prime quality check within LP
python3 mix_effects.py --metric apr --dim1 fee_segment --dim2 is_tprime_borrower --lp-or-mpl LP
```

LP-specific patterns:
- **LP-Tprime grade A** is the highest-volume, lowest-APR cell in the book. When it grows,
  total APR falls significantly through mix. This is often EOM-related.
- **LP Other grade A** can show noisy signals — the segment is small and grade A within it
  is tiny, making percentage moves misleading. Check absolute volume before concluding.
- LP doesn't have investor-level granularity, so FICO is the deepest credit quality cut:
  `python3 mix_effects.py --metric apr --dim1 fico --lp-or-mpl LP`

**If risk grade mix is shifting (E share growing, grade mix moving):**
```bash
python3 mix_effects.py --metric apr --dim1 risk_grade --dim2 channel
python3 mix_effects.py --metric e_share --dim1 fee_segment
python3 mix_effects.py --metric apr --dim1 risk_grade --dim2 is_tprime_borrower
```

**If you suspect a model change:**
```bash
python3 mix_effects.py --metric apr --dim1 model --dim2 risk_grade
```

**If the move looks channel-specific:**
```bash
python3 mix_effects.py --metric apr --dim1 channel --dim2 fee_segment
python3 mix_effects.py --metric apr --dim1 fee_segment --channel dm
python3 mix_effects.py --metric apr --dim1 fee_segment --channel onsite
```

### Step 5 — Synthesize the narrative

**Always lead with a 2–3 sentence executive summary, then provide detail.**
The summary should be self-contained — someone reading only the summary should know what happened.

**Summary format** (2–3 sentences max):
> "[Metric] [up/down] [X]bps WoW ([last]% → [this]%). [One sentence: primary driver — is it
> mix or repricing, which segment, why.] [One sentence: the key secondary effect or a caveat
> if material.]"

Example summary:
> "APR down 5bps WoW (22.98% → 22.93%). Primarily an LP mix story — LP-Tprime grew 10% WoW
> and is lower-APR prime paper, pulling total down through volume mix. MPL appeared to reprice
> up but didn't: Core and Castlelake shuffled grade mix between them rather than changing rate
> sheets; the one genuine pricing signal is FICO 680–700 repricing +81bps."

**Then follow with the detail section**, structured as:
1. Volume context (total $, WoW%, MPL vs LP split)
2. Primary driver explained with numbers
3. Secondary effects and offsets
4. Any data quality flags (ERROR segment, investor near-zero volume, LP Other noise)
5. Related metrics — if APR moved, note what TR and Fee did; if they diverged, flag it

---

## Key Business Context

**Segments (fee_segment):**
- `MPL-Tprime`: Lowest APR (~13%), primer borrowers, lowest loss. Share growth pulls total APR down.
- `MPL Full`: Largest segment (~38% of vol). Standard MPL full-offer.
- `MPL Counter`: Highest APR (~34%), highest loss. Counter-offer flow.
- `LP-Tprime`: Low APR (~9%), prime LP borrowers on T1/T2 strategy.
- `LP Other`: Mid-APR LP loans (~16%).

**Investor pacing (weekly):**
The investor services team actively paces MPL investors on, off, up, and down each week. When an investor goes to zero volume (or surges), this is typically an intentional operational decision — not a data issue. Always frame investor volume changes as likely pacing moves. Note the consequence for grade/APR mix: e.g. taking a high-grade-E investor off-allocation removes high-APR tail paper, shifting MPL grade mix primer and APR downward through vol mix.

**Monthly DM and marketing cycles:**
Near the start of each calendar month, expect a spike in DM (direct mail) traffic. DM start-of-month volume typically brings:
- Lower average FICO borrowers than the steady-state book
- Higher APR and higher counter-offer rates (more MPL Counter flow)
- Larger upward price effects at sub-680 FICO bands
This is a known seasonal pattern. When a week falls at the start of the month and DM share is up, call it out explicitly as the expected monthly DM cycle rather than treating it as an anomaly. The DM surge often partially offsets LP-Tprime mix drag on total APR.

**Common patterns to watch for:**
- **EOM LP throttle**: LP reduces volume end-of-month → MPL picks up prime overflow → MPL looks primer, APR drops through mix. Reverses early next week.
- **TR leads Fee**: Target return changes often precede fee adjustments by a few days. If TR and Fee move in opposite directions, check whether it's a lag — compare pricing_date vs origination_date view.
- **TR/Fee divergence**: If TR rises and Fee falls (or vice versa), this is unusual and worth flagging — it may indicate fee compression from competitive pressure while TR targets held, or a pricing model adjustment not yet fully reflected.
- **Counter% spike**: Usually follows a model change or underwriting expansion.
- **E share growth**: Watch for risk drift, often channel or model related.
- **LP-Tprime A dominating mix**: When prime grade A LP grows, it strongly pulls total APR down (low APR, large volume). Most common driver of total APR mix effects.
- **Grade mix masquerading as repricing**: The most common misread. An investor's average APR can swing 30–70bps WoW purely from grade mix shifting. Always verify with `investor × risk_grade` before calling it a pricing change.
- **FICO as leading indicator**: FICO mix changes this week often predict grade mix changes next week. A shift toward lower FICO → expect E share to rise next week.
- **Investor going dark**: If an investor shows near-zero or zero volume this period where they had volume last period (e.g., all grades showing 0%), flag this explicitly. Could be a throttle, a deal pause, or a data issue. MPL NB is an example of this pattern.
- **ERROR investor segment**: Loans appearing in an `ERROR` investor bucket means originations are not mapping to a known pricing strategy. Always flag this — it's a data quality issue, not a real investor.
- **LP Other grade A noise**: LP Other grade A is a small cell. Large % moves there are often noise. Check absolute volume before drawing conclusions.

**Units:** All metrics stored as decimals. Multiply × 100 for %, × 10,000 for bps.

**Volume note:** `impact_metric_pc` and `impact_volume_pc` should approximately sum to the total WoW change. Large residuals indicate non-linearities (e.g., a segment was zero in one period — the mix effect formula breaks down when a segment goes from zero to non-zero).
