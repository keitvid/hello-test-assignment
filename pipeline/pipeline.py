import logging
import polars as pl

import pipeline.config as config
from pipeline.utils import ingest_source, write_results, SourceData, TransformResults


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ingest(claims: str="data/claims", pharmacies: str="data/pharmacies", reverts: str="data/reverts") -> SourceData:
    """
    Ingest files from the specified folders.
    
    Parameters:
        claims (str):
            Path to claims data folder. Contains json files with main claim events. 
        pharmacies (str):
            Path to pharmacies data folder. Contains csv files with chain->npi mapping for pharmacies.
        reverts (str):
            Path to reverts data folder. Contains json files with revert events. 
    """
    claims_df = ingest_source(claims, config.SOURCES["claims"]["format"], config.SOURCES["claims"]["schema"])
    pharmacies_df = ingest_source(pharmacies, config.SOURCES["pharmacies"]["format"], config.SOURCES["pharmacies"]["schema"])
    reverts_df = ingest_source(reverts, config.SOURCES["reverts"]["format"], config.SOURCES["reverts"]["schema"])

    return SourceData(claims=claims_df, pharmacies=pharmacies_df, reverts=reverts_df)


def transform_staging(claims_df: pl.DataFrame, pharmacies_df: pl.DataFrame, reverts_df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter claims to only include those that have a matching pharmacy.
    """
    df = claims_df.join(pharmacies_df, on="npi", how="inner")
    df = df.join(reverts_df, left_on="id", right_on="claim_id", how="left", suffix="_revert")
    df = df.with_columns(
        (pl.col("price") / pl.col("quantity")).alias("unit_price")
    ).drop("timestamp_revert")

    return df


def transform_metrics(staging_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate metrics for claims (Goal 2)
    """
    df = staging_df \
        .group_by(["ndc", "npi", "chain"]) \
        .agg([
            pl.col("id_revert").is_not_null().sum().alias("reverted"),
            pl.len().alias("fills"),
            pl.mean("unit_price").alias("avg_price"),
            pl.sum("price").alias("total_price")
        ])

    return df


def transform_top_chains(metrics_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate top chains for each drug (Goal 3)
    """
    df = metrics_df.with_columns(
        pl.col("avg_price").rank("ordinal").over("ndc", order_by="avg_price").alias("rank")
    )

    df = df.filter(pl.col("rank") <= config.TOP_CHAINS_THRESHOLD)
    df = df.rename({"chain": "name"})
    df = df.group_by("ndc").agg([
        pl.struct(["name", "avg_price"]).sort_by("name").alias("chain")
    ])

    return df


def transform_most_prescribed_quantity(claims_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate top prescribed quantity for each drug (Goal 4)
    """
    df = claims_df.group_by("ndc", "quantity").agg([
        pl.len().alias("count")
    ])
    df = df.with_columns([
        pl.col("count").rank("ordinal", descending=True).over("ndc", order_by="count").alias("rank")
    ])

    df = df.filter(pl.col("rank") <= config.TOP_QUANTITY_THRESHOLD)
    df = df.group_by("ndc").agg([
        pl.col("quantity").sort_by("quantity").alias("most_prescribed_quantity")
    ])

    return df


def transform(claims_df: pl.DataFrame, pharmacies_df: pl.DataFrame, reverts_df: pl.DataFrame) -> TransformResults:
    """
    Main transform function. Takes all the source dataframes as input and return the result dataframes as a named tuple.
    
    Returns:
        TransformResults: Named tuple with result dataframes
    """
    staging_df = transform_staging(claims_df, pharmacies_df, reverts_df)
    metrics_df = transform_metrics(staging_df)
    top_chains_df = transform_top_chains(metrics_df)
    most_prescribed_quantity_df = transform_most_prescribed_quantity(staging_df)
    metrics_df = metrics_df.drop("chain")

    return TransformResults(
        metrics=metrics_df,
        top_chains=top_chains_df,
        most_prescribed_quantity=most_prescribed_quantity_df
    )


def run_pipeline(claims: str, pharmacies: str, reverts: str) -> None:
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
    logger.info("Starting pipeline")
    source_data = ingest(claims, pharmacies, reverts)

    logger.info("Starting transformations")
    results = transform(source_data.claims, source_data.pharmacies, source_data.reverts)
    
    logger.info("Writing results")
    write_results(results)

    logger.info("Pipeline completed")
