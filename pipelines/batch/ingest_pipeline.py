from src.ingestion.csv_loader import load_social_media_csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def run():
    spark_df = load_social_media_csv(
        spark,
        "/Volumes/workspace/default/test_csv_files/social_media_engagement.csv"
    )

    spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.social_media_engagement_bronze")



if __name__ == "__main__":
    run()