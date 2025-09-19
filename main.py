import fire

from pipeline.pipeline import run_pipeline

def main(claims: str, pharmacies: str, reverts: str) -> None:
    """
    Pharmacy events processing pipeline.

    Parameters:
        claims (str):
            Claims data. Contains json files with main claim events. 
        pharmacies (str):
            Pharmacies data. Contains csv files with chain->npi mapping for pharmacies.
        reverts (str):
            Reverts data. Contains json files with revert events. 
    """
    run_pipeline(claims, pharmacies, reverts)

if __name__ == '__main__':
    fire.Fire(main)
