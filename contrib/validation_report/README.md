# validation_report

Generates a scorecard for yente's matching quality at a given system state (algorithm version, dependencies, data version). Useful for tracking regressions and improvements over time.

## How it works

Three steps:

1. **Generate fixtures** — builds fixture JSONs in `build/fixtures/` from two sources:
   - *True-positives*: Person entities downloaded from OpenSanctions datasets. Optionally applies treatments from `treatments.py` (name typos, character swaps, etc.) to simulate imperfect real-world input.
   - *True-negatives*: Synthetic person records from the reference datasets in `fixtures/`, generated according to the methodology in [opensanctions/qarin](https://github.com/opensanctions/qarin/tree/main/screening-fixtures). In short: records are generated using multi-cultural name generation with realistic data fuzzing and geographic correlation, specifically designed to look plausible without matching any sanctioned entity. `negatives_global.csv` covers diverse global name cultures; `negatives_us.csv` focuses on US-based records.
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

### Step 3: Render the report

```bash
python render_report_data.py build/report_data.json
```

Then open `build/report.html` in a browser.
