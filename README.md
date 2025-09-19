# Pharmacy Events Processing Pipeline

A data processing pipeline for analyzing pharmacy claims, reverts, and pharmacy chain data using Polars.

## Install

1. **Clone or download this repository**

2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the pipeline with default data folders:

```bash
python main.py --claims="path/to/claims" --pharmacies="path/to/pharmacies" --reverts="path/to/reverts"
```

### Output

The pipeline generates three JSON files in the `results/` folder:

1. `metrics.json` - Aggregated metrics by drug (NDC), pharmacy (NPI), and chain
2. `top_chains.json` - Top 2 chains with cheapest average price for each drug
3. `most_prescribed_quantity.json` - Top 5 most prescribed quantities for each drug

## Development

### Changing source data schema

The source data schema is defined in the SOURCES dictionary in the `config.py` file.

### Running Tests

```bash
pytest
```
