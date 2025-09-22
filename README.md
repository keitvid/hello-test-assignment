# Pharmacy Events Processing Pipeline

A data processing pipeline for analyzing pharmacy claims, reverts, and pharmacy chain data.

## Install

1. **Clone or download this repository**

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Run the pipeline in full mode:

In full mode, the pipeline will clean up staging folder and process all files in the claims folder.
This mode is useful for the first run or when you want to reprocess all data.

```bash
python main.py --claims="data/claims" --pharmacies="data/pharmacies" --reverts="data/reverts"
```

2. Run the pipeline in incremental mode:

In incremental mode, the pipeline will only process new files in the claims folder.
This mode is useful for when you want to process only new data.

```bash
python main.py --claims="data/claims" --pharmacies="data/pharmacies" --reverts="data/reverts" --incremental
```

## Output

The pipeline generates three JSON files in the `results/` folder:

1. `metrics.json` - Aggregated metrics by drug (NDC) and pharmacy (NPI)
2. `top_chains.json` - Top 2 chains with cheapest average price for each drug
3. `most_prescribed_quantity.json` - Top 5 most prescribed quantities for each drug
