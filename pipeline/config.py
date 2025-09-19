import polars as pl

RESULTS_DIR = "results"
STAGING_DIR = "staging"

TOP_CHAINS_THRESHOLD = 2
TOP_QUANTITY_THRESHOLD = 5

SOURCES = {
    "claims": {
        "format": "json",
        "schema": {
            "id": pl.String,
            "ndc": pl.String,
            "npi": pl.String,
            "quantity": pl.Float64,
            "price": pl.Float64,
            "timestamp": pl.Datetime
        }
    },  
    "pharmacies": {
        "format": "csv",
        "schema": {
            "chain": pl.String,
            "npi": pl.String
        }
    },
    "reverts": {
        "format": "json",
        "schema": {
            "id": pl.String,
            "claim_id": pl.String,
            "timestamp": pl.Datetime
        }
    }
}