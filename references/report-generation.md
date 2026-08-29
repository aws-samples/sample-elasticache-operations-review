# Report generation — putting your analysis in the report, and pricing the savings

Read this when rendering the HTML report (`scripts/generate_html_report.py`): how the
agent's synthesis reaches the report through `output/notes.json`, the rules the renderer
enforces on it, and how to supply live-priced savings via `--pricing`.

## Putting your own analysis in the report

The report renders the scripts' numbers, and **leads with your synthesis** —
your assessment is the first thing in "What the data shows", labeled AI-generated.
Which findings are real, what to do first, whether this is production reaches the
report only through the review, so writing it is **required** for every report, not
an optional add-on. Write it to **`output/notes.json`** and the renderer picks it up
automatically — no flag needed (an explicit `--notes <path>` still works):

```bash
# Write output/notes.json (Step 4), then just render — notes.json is auto-included:
python3 scripts/generate_html_report.py --output output/report.html
# (equivalently, pass it explicitly:)
python3 scripts/generate_html_report.py --notes output/notes.json --output output/report.html
```

```json
{
  "assessment": {"prose": "...", "cites": ["clusters.<id>.percentiles.<Metric>_<Stat>"]},
  "context": {"environment": "confirmed non-production", "source": "user"},
  "finding_notes": [{"finding_id": "<from analysis.json or config_findings.json>",
                     "verdict": "confirmed|false_positive|needs_data",
                     "reasoning": "...", "cites": ["..."]}],
  "priorities": [{"rank": 1, "action": "...", "why": "...", "cites": ["..."]}]
}
```

Four rules the renderer enforces, so a failed render is a notes problem and never
a silent omission:

- **Every figure in your prose must appear under a path that note cites.** Not
  somewhere in the JSON — under the cited path. A number that does not match
  fails the render and is named in the error. Round freely: "0.7" matches 0.67,
  but "0.6712" asserts precision the source lacks and is rejected.
- **Cite `analysis.json`, `inventory.json`, `config_findings.json`, or (when
  present) `pricing.json`, never raw `metrics.json`** — it is not citable. One raw
  metric series holds thousands of datapoints and would authorise almost any
  percentage. Cite the percentile entry that summarises it; cite a priced figure
  as `clusters.<id>.current_monthly` or an option's `monthly`/`saving_monthly`.
- **Cite the specific value, not the cluster.** A note whose cites reach more
  than 250 distinct numbers is rejected as too broad to check.
- **`finding_id` must exist.** A note naming an unknown id fails rather than
  quietly annotating nothing.

A `false_positive` verdict does not delete the row — it renders struck through
with your reasoning beside it, because deleting it would hide a pipeline bug
behind your judgement. Omit `--notes` entirely and the report still renders every
data section, and the lead of "What the data shows" states plainly that no AI
review was generated for this run — an absent reading announced, never hidden.

## Pricing the savings — `--pricing pricing.json`

Exact dollar savings need **live** rates, which must not enter the reproducible
pipeline. So the pricing is a separate, presentation-time input you build with the
`amazon-elasticache` toolkit's `price_calculator.py` and pass to the report:

```bash
python3 scripts/generate_html_report.py --pricing output/pricing.json --output output/report.html
```

For each node-based replication group, run `price_calculator.py --mode node
--node-type <t> --nodes <n> --engine redis|valkey --region <r>` (and
`--show-ri-options` for reserved) and record the figures in `pricing.json` per the
schema the report reads (`clusters.<id>.current_monthly` + an `options` list of
`{key,label,monthly,saving_monthly,eligible,commitment,note}`), with a `metadata`
block naming `source`, `region`, and `retrieved_at`. Rules:

- **Apply the sanity guard.** `price_calculator` returns anomalous rates for some
  node/engine combinations (a Valkey rate far below the same node's Redis rate is a
  known bad SKU match). Reject any Valkey rate >40% below the Redis rate and omit
  that lever's dollar rather than shipping a wrong number.
- **The Database Savings Plan is Valkey + Gen7+ only** (20% off instances, 30% off
  serverless), a published ratio — not a price. State it as eligibility + ratio when
  the pricer cannot produce a trustworthy Gen7-Valkey rate.
- **The report gates the levers, you supply the prices.** It shows decommission for
  IDLE clusters (recovering current spend) and suppresses commitments there; on
  active clusters it shows the keep-and-optimize levers and flags a commitment as
  risky when `steadiness` is `variable`/`spiky`. Never recommend a commitment on an
  idle or spiky cluster, and never sum the levers — they are alternatives.

Omit `--pricing` and the report renders without the savings KPIs or cost options —
never a fabricated saving.
