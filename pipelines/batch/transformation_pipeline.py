from src.transformations.imputation import (compute_imputation_stats, impute_metrics)
from src.transformations.aggregation import (add_engagement_rate)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean

spark = SparkSession.builder.getOrCreate()

def run():
    # TODO: avoid reading the whole table (probably passing ingestion batch id as filter)
    spark_df = spark.read.table("workspace.bronze.social_media_engagement_bronze")

    spark_df = (
        spark_df
        .withColumn(
            "likes_was_null",
            col("likes").isNull()
        )
        .withColumn(
            "comments_was_null",
            col("comments").isNull()
        )
        .withColumn(
            "shares_was_null",
            col("shares").isNull()
        )
        .withColumn(
            "reach_was_null",
            col("reach").isNull()
        )
    )

    stats = compute_imputation_stats(spark_df)

    spark_df = impute_metrics(spark_df, stats)

    spark_df = add_engagement_rate(spark_df)


    # TODO: update it do upsertt/merge operation
    spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.social_media_engagement_silver")



if __name__ == "__main__":
    run()