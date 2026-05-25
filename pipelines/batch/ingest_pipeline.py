from src.ingestion.csv_loader import load_social_media_csv

from pyspark.sql.functions import (current_timestamp, lit)
from pyspark.sql import SparkSession

from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def generate_batch_id():
    return datetime.utcnow().strftime("%Y%m%d%H%M%S")

def run(source_path):
    spark_df = load_social_media_csv(
        spark,
        source_path
    )

    spark_df = (
        spark_df
        .withColumn(
            "_txn_source",
            lit("csv")
        )
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_ingestion_batch_id",
            lit(generate_batch_id())
        )
    )

    spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.social_media_engagement_bronze")



if __name__ == "__main__":
    run()