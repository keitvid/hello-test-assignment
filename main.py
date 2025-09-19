import fire
import logging
from rich.logging import RichHandler
from enum import Enum

from pipeline.transform import run_pipeline

logging.basicConfig(level="INFO", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()])
logger = logging.getLogger(__name__)


def main(claims: str, pharmacies: str, reverts: str, incremental: bool=False, loglevel: str="INFO") -> None:
    """
    Claim events processing pipeline.

    Parameters:
        claims (str):
            Claims data. Contains json files with main claim events. 
        pharmacies (str):
            Pharmacies data. Contains csv files with chain->npi mapping for pharmacies.
        reverts (str):
            Reverts data. Contains json files with revert events.
        incremental(bool):
            If True, only process new files.
        loglevel (str):
            Logging level. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL
    """
    logging.getLogger().setLevel(loglevel.upper())
    run_pipeline(claims, pharmacies, reverts, incremental)

if __name__ == '__main__':
    fire.Fire(main)
