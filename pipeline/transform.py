import logging
import polars as pl

import pipeline.config as config
from pipeline.ingest import ingest_source, ingest_claims, SourceData
from pipeline.write import write_results, TransformResults


logger = logging.getLogger(__name__)


def ingest(
    claims: str,
    pharmacies: str,
    reverts: str,
    incremental: bool
    ) -> pl.LazyFrame:
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
    pharmacies_df = ingest_source(pharmacies, format=config.SOURCES["pharmacies"]["format"], schema=config.SOURCES["pharmacies"]["schema"])
    reverts_df = ingest_source(reverts, format=config.SOURCES["reverts"]["format"], schema=config.SOURCES["reverts"]["schema"])

    ingest_claims(
        claims,
        config.SOURCES["claims"]["format"],
        config.SOURCES["claims"]["schema"],
        pharmacies_df,
        incremental
    )

    return SourceData(pl.scan_parquet(config.STAGING_DIR), pharmacies_df.lazy(), reverts_df.lazy()) 


def transform_staging(
    claims_lf: pl.LazyFrame,
    pharmacies_df: pl.DataFrame,
    reverts_df: pl.DataFrame
    ) -> pl.DataFrame:
    """
    Create staging table
    """
    lf = claims_lf.join(pharmacies_df, on="npi", how="inner")
    lf = lf.join(reverts_df, left_on="id", right_on="claim_id", how="left", suffix="_revert")
    lf = lf.with_columns(
        (pl.col("price") / pl.col("quantity")).alias("unit_price")
    ).drop("timestamp_revert")

    return lf

def transform_metrics(staging_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate metrics for claims (Goal 2)
    """
    lf = staging_lf \
        .group_by(["ndc", "npi", "chain"]) \
        .agg([
            pl.col("id_revert").is_not_null().sum().alias("reverted"),
            pl.len().alias("fills"),
            pl.mean("unit_price").alias("avg_price"),
            pl.sum("price").alias("total_price")
        ])

    return lf


def transform_top_chains(metrics_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate top chains for each drug (Goal 3)
    """
    lf = metrics_lf.with_columns(
        pl.col("avg_price").rank("ordinal").over("ndc", order_by="avg_price").alias("rank")
    )

    lf = lf.filter(pl.col("rank") <= config.TOP_CHAINS_THRESHOLD)
    lf = lf.rename({"chain": "name"})
    lf = lf.group_by("ndc").agg([
        pl.struct(["name", "avg_price"]).sort_by("name").alias("chain")
    ])

    return lf


def transform_most_prescribed_quantity(staging_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate top prescribed quantity for each drug (Goal 4)
    """
    lf = staging_lf.group_by("ndc", "quantity").agg([
        pl.len().alias("count")
    ])
    lf = lf.with_columns([
        pl.col("count").rank("ordinal", descending=True).over("ndc", order_by="count").alias("rank")
    ])

    lf = lf.filter(pl.col("rank") <= config.TOP_QUANTITY_THRESHOLD)
    lf = lf.group_by("ndc").agg([
        pl.col("quantity").sort_by("quantity").alias("most_prescribed_quantity")
    ])

    return lf


def transform(claims_lf: pl.LazyFrame, pharmacies_lf: pl.DataFrame, reverts_lf: pl.DataFrame) -> TransformResults:
    """
    Main transform function. Takes all the source dataframes as input and return the result dataframes as a named tuple.
    
    Returns:
        TransformResults: Named tuple with result dataframes
    """
    staging_lf = transform_staging(claims_lf, pharmacies_lf, reverts_lf)
    metrics_lf = transform_metrics(staging_lf)
    top_chains_lf = transform_top_chains(metrics_lf)
    most_prescribed_quantity_lf = transform_most_prescribed_quantity(staging_lf)
    metrics_lf = metrics_lf.drop("chain")

    return TransformResults(
        metrics=metrics_lf,
        top_chains=top_chains_lf,
        most_prescribed_quantity=most_prescribed_quantity_lf
    )


def run_pipeline(claims: str, pharmacies: str, reverts: str, incremental: bool=False) -> None:
    """
    Main processing pipeline.
    """
    logger.info("Starting pipeline")
    logger.info(f"Running in {'incremental' if incremental else 'full'} mode")
    logger.info("Ingesting raw data")
    source_data = ingest(claims, pharmacies, reverts, incremental)

    logger.info("Starting transformations")
    results = transform(source_data.claims, source_data.pharmacies, source_data.reverts)
    
    logger.info("Writing results")
    write_results(results)

    logger.info("Pipeline finished")
