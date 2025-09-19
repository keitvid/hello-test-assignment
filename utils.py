import polars as pl
import os
from collections import namedtuple
import logging


from config import RESULTS_DIR


logger = logging.getLogger(__name__)


SourceData = namedtuple('SourceData', ['claims', 'pharmacies', 'reverts'])
TransformResults = namedtuple('TransformResults', ['metrics', 'top_chains', 'most_prescribed_quantity'])


def ingest_source(folder: str, format: str, schema: dict) -> pl.DataFrame:
    """
    Ingest files from the specified folder.
    """
    if not os.path.exists(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = os.listdir(folder)
    dfs = []
    for file in files:
        if not file.endswith(f".{format}"):
            logger.warning(f"Skipping file: {file}")
            continue

        logger.debug(f"Processing file: {file}")

        if format == "csv":
            df = pl.read_csv(os.path.join(folder, file), schema=schema)
        elif format == "json":
            df = pl.read_json(os.path.join(folder, file), schema=schema)
        else:
            raise ValueError(f"Unsupported format: {format}")

        dfs.append(df)

    return pl.concat(dfs)


def write_results(results: TransformResults) -> None:
    """
    Write result dataframes to json files.
    
    Parameters:
        results (TransformResults):
            Result dataframes to write.
    """
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)

    for name, df in results._asdict().items():
        df.write_json(f"{RESULTS_DIR}/{name}.json")
