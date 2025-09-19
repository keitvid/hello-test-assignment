import polars as pl
import os
import logging
from collections import namedtuple

from pipeline.config import RESULTS_DIR, STAGING_DIR


SourceData = namedtuple('SourceData', ['claims', 'pharmacies', 'reverts'])


logger = logging.getLogger(__name__)


def read_file(path: str, format: str, schema: dict, filter_nulls = True):
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
    Ingest files from the specified folder.
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = os.listdir(folder)
    dfs = []
    for file in files:
        df = read_file(os.path.join(folder, file), format=format, schema=schema)
        dfs.append(df)

    return pl.concat(dfs)


def ingest_claims(
    folder: str,
    format: str,
    schema: dict,
    pharmacies_df,
    incremental: bool):
    """
    Ingest files from the specified folder.
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

