from pipeline.transform import ingest
import polars as pl
from polars.testing import assert_frame_equal


CLAIMS_SCHEMA = {
    "id": pl.String,
    "ndc": pl.String,
    "npi": pl.String,
    "quantity": pl.Float64,
    "price": pl.Float64,
    "timestamp": pl.Datetime
}

def test_ingest_claims():
    """
    Test ingest claims
    """
    # Act
    source_data = ingest(
        'tests/test_data/claims',
        'tests/test_data/pharmacies',
        'tests/test_data/reverts',
        False)

    # Assert
    expected_df = pl.DataFrame({
            "id":        ["c0000", "c0002"],
            "ndc":       ["d0000", "d0000"],
            "npi":       ["p0000", "p0001"],
            "quantity":  [10.0, 10.0],
            "price":     [100.0, 100.0],
            "timestamp": ["2024-01-01T00:00:00", "2024-01-01T00:00:00"]
    }, schema=CLAIMS_SCHEMA)

    print(source_data.claims.collect())
    assert_frame_equal(source_data.claims.collect(), expected_df)


def test_ingest_pharmacies():
    """
    Test ingest pharmacies
    """
    # Act
    source_data = ingest(
        'tests/test_data/claims',
        'tests/test_data/pharmacies',
        'tests/test_data/reverts',
        False)

    # Assert
    expected_df = pl.DataFrame({
        "chain": ["health", "saint"],
        "npi":   ["p0000", "p0001"]
    },
    schema={
        "chain": pl.String,
        "npi": pl.String,
    })

    assert_frame_equal(source_data.pharmacies.collect(), expected_df)


def test_ingest_reverts():
    """
    Test ingest reverts
    """
    # Act
    source_data = ingest(
        'tests/test_data/claims',
        'tests/test_data/pharmacies',
        'tests/test_data/reverts',
        False)

    # Assert
    expected_df = pl.DataFrame({
        "id":        ["r0000"],
        "claim_id":  ["c0000"],
        "timestamp": ["2024-01-01T00:00:00"]
    },
    schema={
        "id": pl.String,
        "claim_id": pl.String,
        "timestamp": pl.Datetime
    })

    assert_frame_equal(source_data.reverts.collect(), expected_df)
