import polars as pl
import os
import logging
from collections import namedtuple

from pipeline.config import STAGING_DIR, SOURCES


SourceData = namedtuple('SourceData', ['claims', 'pharmacies', 'reverts'])


logger = logging.getLogger(__name__)


def read_file(path: str, format: str, schema: dict, filter_nulls = True) -> pl.DataFrame:
    """
    Read a single file from the specified path.

    Returns:
        pl.DataFrame: DataFrame with file data
    """
    if format == "csv":
        df = pl.read_csv(path, schema=schema)
    elif format == "json":
        df = pl.read_json(path, schema=schema)
    else:
        raise ValueError(f"Unsupported format: {format}")

    if filter_nulls:
        df = df.filter(pl.all_horizontal(pl.all().is_not_null()))

    return df


def ingest_source(folder: str, format: str, schema: dict, filter_nulls: bool = True) -> pl.DataFrame:
    """
    Ingest all files from the specified folder.

    Returns:
        pl.DataFrame: DataFrame with source data
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = os.listdir(folder)
    dfs = []
    for file in files:
        df = read_file(os.path.join(folder, file), format=format, schema=schema, filter_nulls=filter_nulls)
        dfs.append(df)

    return pl.concat(dfs)


def ingest_claims(
    folder: str,
    format: str,
    schema: dict,
    pharmacies_df,
    incremental: bool) -> pl.LazyFrame:
    """
    Ingest claims files from the specified folder.
    Claims, as a main events source, can be ingested in two modes: full and incremental
    In order to reduce processing resources, claims are ingested in parquet format.

    Returns:
        pl.LazyFrame: LazyFrame with claims data
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")
    
    if not os.path.exists(STAGING_DIR):
        os.makedirs(STAGING_DIR)

    source_files = os.listdir(folder)
    staging_files = os.listdir(STAGING_DIR)

    if incremental:
        new_files = [source_file for source_file in source_files if f"{os.path.splitext(source_file)[0]}.parquet" not in staging_files]
    else:
        for file in staging_files:
            os.remove(os.path.join(STAGING_DIR, file))
    
        new_files = source_files

    for file in new_files:
        logger.debug(f"Processing file: {file}")
        raw_claims_df = read_file(os.path.join(folder, file), format=format, schema=schema)
        filtered_claims_df = raw_claims_df.filter(pl.col("npi").is_in(pharmacies_df["npi"].implode()))
        filtered_claims_df.write_parquet(f'{STAGING_DIR}/{os.path.splitext(file)[0]}.parquet')

    return pl.scan_parquet(STAGING_DIR)

def ingest(
    claims: str,
    pharmacies: str,
    reverts: str,
    incremental: bool
    ) -> SourceData:
    """
    Ingest source data from the specified folders.
    
    Parameters:
        claims (str):
            Path to claims data folder. Contains json files with main claim events. 
        pharmacies (str):
            Path to pharmacies data folder. Contains csv files with chain->npi mapping for pharmacies.
        reverts (str):
            Path to reverts data folder. Contains json files with revert events.

    Returns:
        SourceData: Named tuple with source lazyframes
    """
    pharmacies_df = ingest_source(pharmacies, format=SOURCES["pharmacies"]["format"], schema=SOURCES["pharmacies"]["schema"])
    reverts_df = ingest_source(reverts, format=SOURCES["reverts"]["format"], schema=SOURCES["reverts"]["schema"])

    claims_lf = ingest_claims(
        claims,
        SOURCES["claims"]["format"],
        SOURCES["claims"]["schema"],
        pharmacies_df,
        incremental
    )

    return SourceData(claims_lf, pharmacies_df.lazy(), reverts_df.lazy()) 
