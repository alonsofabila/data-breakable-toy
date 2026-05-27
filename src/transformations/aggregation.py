from pyspark.sql.functions import expr

# TODO: add comments
def add_engagement_rate(df):
    return df.withColumn(
        "engagement_rate",
        expr("""
            (likes + comments + shares) / reach
        """)
    )