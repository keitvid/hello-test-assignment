from collections import namedtuple
import os
from pipeline.config import RESULTS_DIR
import logging


logger = logging.getLogger(__name__)
TransformResults = namedtuple('TransformResults', ['metrics', 'top_chains', 'most_prescribed_quantity'])


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
        logger.info(f"Writing {name} to {RESULTS_DIR}/{name}.json")
        df.collect().write_json(f"{RESULTS_DIR}/{name}.json")