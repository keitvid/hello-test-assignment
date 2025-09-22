import logging
import polars as pl

import pipeline.config as config
from pipeline.ingest import ingest
from pipeline.write import write_results, TransformResults


logger = logging.getLogger(__name__)


def transform_staging(
    claims_lf: pl.LazyFrame,
    pharmacies_df: pl.DataFrame,
    reverts_df: pl.DataFrame
    ) -> pl.LazyFrame:
    """
    Create staging dataframe. This dataframe will be used for all further transformations. 
    """
    # Get chain name. Filtering is done during ingestion.
    lf = claims_lf.join(pharmacies_df, on="npi", how="inner")

    # Get reverts
    lf = lf.join(reverts_df, left_on="id", right_on="claim_id", how="left", suffix="_revert")

    # Calculate unit price
    lf = lf.with_columns(
        (pl.col("price") / pl.col("quantity")).alias("unit_price")
    ).drop("timestamp_revert")

    return lf


def transform_metrics(staging_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate metrics for claims (Goal 2)
    """

    # Calculate metrics per chain/drug:
    # - reverted: number of reverted claims
    # - fills: number of claims
    # - avg_price: average unit price
    # - total_price: total price
    lf = staging_lf \
        .group_by(["ndc", "npi", "chain"]) \
        .agg([
            pl.col("id_revert").is_not_null().sum().alias("reverted"),
            pl.len().alias("fills"),
            pl.mean("unit_price").round(2).alias("avg_price"),
            pl.sum("price").round(2).alias("total_price")
        ])

    return lf


def transform_top_chains(metrics_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Goal 3: Calculate chains with less average unit price for each drug
    """
    # Rank reach chain per drug by average unit price.  
    lf = metrics_lf.with_columns(
        pl.col("avg_price").rank("ordinal").over("ndc", order_by="avg_price").alias("rank")
    )

    # Get chains with less average unit price for each drug
    lf = lf.filter(pl.col("rank") <= config.TOP_CHAINS_THRESHOLD)

    # Form struct with chain name and average unit price
    lf = lf.rename({"chain": "name"})
    lf = lf.group_by("ndc").agg([
        pl.struct(["name", "avg_price"]).sort_by("name").alias("chain")
    ])

    return lf


def transform_most_prescribed_quantity(staging_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Goal 4: Calculate top prescribed quantity for each drug
    """
    # Calculate quantity frequency per drug
    lf = staging_lf.group_by("ndc", "quantity").agg([
        pl.len().alias("count")
    ])

    # Rank quantity frequency per drug
    lf = lf.with_columns([
        pl.col("count").rank("ordinal", descending=True).over("ndc", order_by="count").alias("rank")
    ])

    # Get top quantity frequency per drug
    lf = lf.filter(pl.col("rank") <= config.TOP_QUANTITY_THRESHOLD)

    # Form top quantity list column
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
    Main data processing pipeline.
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
