# validation_report

Generates a scorecard for yente's matching quality at a given system state (algorithm version, dependencies, data version). Useful for tracking regressions and improvements over time.

## How it works

Three steps:

1. **Generate fixtures** — downloads Person entities from OpenSanctions datasets and writes true-positive fixture JSONs to `build/fixtures/`. Each fixture pulls from a specific dataset and optionally applies treatments from `treatments.py` (name typos, character swaps, etc.) to simulate imperfect real-world input. The generated JSONs record the dataset version they were built from. Only needs re-running when you want fresh data.
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
