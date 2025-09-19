import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from main import transform
from polars.testing import assert_frame_equal


def test_metrics():
    """Test metrics."""
    # Arrange
    claims_df = pl.DataFrame({
        "id":        ["c1000",      "c1001",      "c1002"],
        "ndc":       ["d1000",      "d1001",      "d1001"],
        "npi":       ["p1000",      "p1000",      "p1000"],
        "quantity":  [10.0,         20.0,         22.0],
        "price":     [100.0,        200.0,        264.0],
        "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })

    pharmacies_df = pl.DataFrame({
        "chain": ["chain_1"],
        "npi":   ["p1000"]
    })
    
    reverts_df = pl.DataFrame({
        "id":        ["r1000"],
        "claim_id":  ["c1000"],
        "timestamp": ["2024-04-02T21:41:19"]
    })

    # Act
    results = transform(claims_df, pharmacies_df, reverts_df)

    # Assert
    expected_metrics_df = pl.DataFrame({
        "ndc":         ["d1000", "d1001"],
        "npi":         ["p1000", "p1000"],
        "reverted":    [1,       0],
        "fills":       [1,       2],
        "avg_price":   [10.0,    11.0],
        "total_price": [100.0,   464.0]
    },
    schema={
        "npi": pl.String,
        "ndc": pl.String,
        "fills": pl.UInt32,
        "reverted": pl.UInt32,
        "avg_price": pl.Float64,
        "total_price": pl.Float64
    })

    assert_frame_equal(
        results.metrics,
        expected_metrics_df,
        check_row_order=False,
        check_column_order=False
        )


def test_top_chains():
    """Test top chains."""
    # Arrange
    claims_df = pl.DataFrame({
        "id":        ["c1000",      "c1001",      "c1002"],
        "ndc":       ["d1000",      "d1000",      "d1000"],
        "npi":       ["p1000",      "p1001",      "p1002"],
        "quantity":  [10.0,         20.0,         10.0],
        "price":     [100.0,        300.0,        200.0],
        "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"]
    })

    pharmacies_df = pl.DataFrame({
        "chain": ["chain_1", "chain_2", "chain_3"],
        "npi":   ["p1000",   "p1001",   "p1002"]
    })
    
    reverts_df = pl.DataFrame({
        "id":        ["r1000"],
        "claim_id":  ["c1000"],
        "timestamp": ["2024-04-02T21:41:19"]
    })
    
    # Act
    results = transform(claims_df, pharmacies_df, reverts_df)

    # Assert
    expected_top_chains_df = pl.DataFrame({
        "ndc":         ["d1000"],
        "chain":       [[
            {
                "name": "chain_1",
                "avg_price": 10.0
            },
            {
                "name": "chain_2",
                "avg_price": 15.0
            }
        ]],
    })

    assert_frame_equal(
        results.top_chains,
        expected_top_chains_df,
        check_row_order=False,
        check_column_order=False
        )


def test_most_prescribed_quantity():
    """Test most prescribed quantity."""
    # Arrange
    claims_df = pl.DataFrame({
        "id":        ["c1000",      "c1001",      "c1002",      "c1003",      "c1004",      "c1005",      "c1006",      "c1007"],
        "ndc":       ["d1000",      "d1000",      "d1000",      "d1000",      "d1000",      "d1000",      "d1000",      "d1000"],
        "npi":       ["p1000",      "p1001",      "p1001",      "p1002",      "p1002",      "p1002",      "p1002",      "p1002"],
        "quantity":  [10.0,         10.0,         20.0,         20.0,         30.0,         30.0,         40.0,         50.0],
        "price":     [100.0,        200.0,        264.0,        328.0,        400.0,        476.0,        552.0,        628.0],
        "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07", "2023-01-08"]
    })

    pharmacies_df = pl.DataFrame({
        "chain": ["chain_1", "chain_2", "chain_3"],
        "npi":   ["p1000",   "p1001",   "p1002"]
    })
    
    reverts_df = pl.DataFrame({
        "id":        ["r1000"],
        "claim_id":  ["c1000"],
        "timestamp": ["2024-04-02T21:41:19"]
    })
    
    # Act
    results = transform(claims_df, pharmacies_df, reverts_df)

    # Assert
    expected_most_prescribed_quantity_df = pl.DataFrame({
        "ndc":         ["d1000"],
        "most_prescribed_quantity": [[
            10.0, 
            20.0, 
            30.0, 
            40.0, 
            50.0
        ]],
    })

    assert_frame_equal(
        results.most_prescribed_quantity,
        expected_most_prescribed_quantity_df,
        check_row_order=False,
        check_column_order=False
        )