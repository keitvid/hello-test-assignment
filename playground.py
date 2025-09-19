import polars as pl


df = pl.DataFrame({
    "ndc":         ["d1000", "d1000", "d1000", "d1000", "d1000"],
    "quantity":    [50.0, 20.0, 30.0, 10.0, 12.0],
    "rank":        [1, 2, 3, 4, 5]
})

df = df.group_by("ndc").agg([
    pl.col("quantity").sort_by("rank").alias("most_prescribed_quantity")
])

df.write_json("results/most_prescribed_quantity.json")

