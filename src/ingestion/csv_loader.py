from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType
)

SOCIAL_MEDIA_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("post_timestamp", TimestampType(), True),
    StructField("likes", DoubleType(), True),
    StructField("comments", DoubleType(), True),
    StructField("shares", DoubleType(), True),
    StructField("reach", DoubleType(), True)
])


def load_social_media_csv(spark, path):
    return (
        spark.read.format("csv")
        .schema(SOCIAL_MEDIA_SCHEMA)
        .option("header", True)
        .load(path)
    )