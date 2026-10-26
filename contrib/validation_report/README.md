# validation_report

Generates a scorecard for yente's matching quality at a given system state (algorithm version, dependencies, data version). Useful for tracking regressions and improvements over time.

## How it works

Three steps:

1. **Generate fixtures** — builds fixture JSONs in `build/fixtures/` from two sources:
   - *True-positives*: Person entities downloaded from OpenSanctions datasets. Optionally applies treatments from `treatments.py` (name typos, character swaps, etc.) to simulate imperfect real-world input.
   - *True-negatives*: Synthetic person records from the reference datasets in `fixtures/`, generated according to the methodology in [opensanctions/qarin](https://github.com/opensanctions/qarin/tree/main/screening-fixtures). In short: records are generated using multi-cultural name generation with realistic data fuzzing and geographic correlation, specifically designed to look plausible without matching any sanctioned entity. `negatives_global.csv` covers diverse global name cultures; `negatives_us.csv` focuses on US-based records.
   - *Address positives*: legal entities with a street-level address from `us_ofac_sdn` and `ua_nsdc_sanctions` (125 each), queried with name and address treated, and by the treated address alone.
   - *Address negatives*: synthetic names with the address of a listed entity, and with near misses of listed addresses — another house number, street, country or region.

   Each fixture lists the runs it is reported for (algorithm and feature weights, e.g. logic-v2 with `address_match` weighted), and its description says how to read them.
2. **Generate report data** — runs each fixture against a live yente instance, collects scores and ID-recall stats, writes a JSON report.
3. **Render report** — turns the JSON report into a self-contained HTML file.

## Usage

### Step 1: Generate fixtures

```bash
python generate_fixtures.py
```

### Step 2: Generate report data

Requires a running yente instance.

```bash
python generate_report_data.py
```

Each fixture runs with the algorithms and weights listed in its `runs`. Against
a yente version without `address_match`, the weighted run equals plain
logic-v2. The weight is
set in `generate_fixtures.py` and must be above the 0.7 threshold for an
address-only match to be returned: `/match` drops results scoring at or below
the cutoff, which defaults to the threshold.

### Step 3: Render the report

```bash
python render_report_data.py build/report_data.json
```

Then open `build/report.html` in a browser.


## Comparing runs

Write each yente server's report data and HTML to their own files, and
compare the reports:

```bash
python generate_report_data.py --base-url http://localhost:8001 --output build/main.json
python generate_report_data.py --base-url http://localhost:8002 --output build/branch.json
python render_report_data.py build/main.json --output build/main.html
python render_report_data.py build/branch.json --output build/branch.html
```
