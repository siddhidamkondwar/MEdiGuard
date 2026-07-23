# MediGuard AI

Explainable hospital-bill fraud detection on a Delta Lakehouse.

## Current status
- **Step 1 — Contracts frozen.** Canonical claim-line schema, 9 Delta table schemas,
  single-writer registry, verdict contract, all proven by `tests/test_contracts.py`.
- **Step 2 — Walking skeleton.** Fake claim-lines flow end to end:
  generate → Delta write → read back → stub verdict. Proves the plumbing.
- **Step 3 — Ingestion gate (first REAL component).** Messy raw hospital bills →
  clean canonical rows. Maps columns, strips PII (salt-hashes patient id, drops
  names), translates foreign codes to Indian (SNOMED→ICD-10, CPT→HBP + rate),
  validates against the contract, and quarantines bad rows with reasons. Covered by
  `tests/test_ingestion.py`. Run it: `python run_ingestion.py`
- **Step 4 — Rules & baseline engine (first fraud detection).** Mines population
  baselines (diagnosis-procedure norms, cost percentiles) from the corpus, then runs
  four deterministic checks per claim: overbilling vs HBP rate, diagnosis-procedure
  fit, stay logic, and cost outliers. Every finding is explainable and rupee-exact.
  Covered by `tests/test_rules.py`. Run it: `python run_rules.py`
- **Step 5 — Cost model (last deterministic evidence computer).** Estimates a fair
  price band (p25-p95) per line from comparable real bills and flags amounts sitting
  far above it — catching inflated bills that stay under the HBP cap. Feeds the ML
  layer next. Covered by `tests/test_cost_model.py`. Run it: `python run_cost_model.py`
- **Step 6 — Feature engineering.** Packs all deterministic evidence into ONE fixed
  numeric row per claim (21 features, frozen order in `FEATURE_NAMES`). Includes
  normalised ratios, not just raw rupees. Covered by `tests/test_features.py`.
  Run it: `python run_features.py`

- **Step 7 — Realistic labelled data + ML model.** Replaced the unrealistic generator
  with one producing ~8% rare, labelled fraud (upcoding, phantom services, impossible
  stays, quantity inflation) plus natural noise so rules produce genuine false
  positives. Added materiality thresholds to R1/R4 so trivial overages no longer fire.
  Trained a LightGBM model that weighs the deterministic signals, with SHAP
  explanations in plain language. Covered by `tests/test_ml.py`.
  Run: `python rebuild_data.py` then `python run_ml.py`
- **Step 8 — Anomaly detection.** Unsupervised Isolation Forest over *behavioural*
  profiles (billing intensity, money concentration, procedure mix) — deliberately NOT
  rule outputs, so it can catch what the rules cannot. Covered by
  `tests/test_anomaly.py`. Run: `python run_anomaly.py`
  - Also fixed a **label-noise bug** found during this step: claims could be labelled
    fraud when the injected pattern never actually landed on a line. Labels now record
    only patterns that genuinely applied.

## Results (synthetic test set, 600 claims, 44 fraud)
| | precision | recall | F1 |
|---|---|---|---|
| Rules only | 0.48 | 1.00 | 0.65 |
| **+ ML model** | **1.00** | **0.95** | **0.98** |

Rules catch all fraud but flag ~2 honest claims per real one. The model keeps recall
high and removes the false alarms. **The ML layer's value is filtering noise, not
finding new fraud.**

### Anomaly detection: held-out "unbundling" test
A fraud type no rule checks — one episode split into many small lines, each valid, at
rate, and matching the diagnosis:

| | caught |
|---|---|
| Rules | 0 / 40 |
| **Anomaly detection** | **40 / 40** |

This is why the component exists: it judges the claim's *shape*, not its rule breaches.

## HONEST LIMITATIONS — read before quoting any number above
1. **ROC-AUC of 1.00 is a warning sign, not a triumph.** Every fraud pattern injected
   has a rule written to catch it, so once labels were clean the task became trivially
   separable. These figures are an **upper bound on a synthetic benchmark**, not a
   prediction of real-world performance, which would be substantially lower.
2. **Claim size partly correlates with the label** (fraudulent claims run larger by
   construction), so the model uses size as a secondary signal. Rule findings still
   dominate feature importance.
3. **Anomaly detection adds zero recall on the main dataset** — the rules already
   catch 100% of injected fraud there. Its value is demonstrated only on the held-out
   unbundling test, i.e. on fraud types outside the rule set.
4. **The crosswalk tables are small samples**, not the full WHO ICD-10 / NHA HBP 2.2
   sets.
5. **No real audit labels exist.** In production, labels come from investigator
   outcomes; here they come from the generator that created the fraud.

## Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Contract test (fast, no Spark needed for the logic itself):
```bash
pytest tests/test_contracts.py -v
```

Walking skeleton — Spark + DuckDB (needs internet the first time to fetch the Delta
plugin from Maven; it caches afterwards):
```bash
python orchestrate.py
```

Walking skeleton — offline fallback (no Maven / no DuckDB extension download).
Uses delta-rs to stand in for the Spark write and DuckDB read, producing the same
real Delta format:
```bash
python orchestrate_sandbox.py
```

## Privacy note
The ingestion gate salt-hashes patient ids. For real data, set a secret salt:
```bash
export PATIENT_SALT="your-secret-value"   # never commit this
```
Without it, a clearly-marked dev default is used (fine for synthetic data only).

## Local streaming broker (used later)
```bash
docker compose up -d          # Redpanda on localhost:19092
```

## What is real vs stub right now
| Piece | State |
|---|---|
| Contracts (schemas, writer registry, verdict) | real, frozen |
| Delta write + read | real Delta format |
| `build_corpus` | stub — writes fake rows; ingestion-gate cleaning added later |
| `reasoner` | stub — caps at HBP rate; no ML, no LLM yet |
| Everything in `ml/`, real batch mining, streaming | not built yet |
