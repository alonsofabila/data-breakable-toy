from pyspark.sql.functions import mean


# TODO: add comments
def compute_imputation_stats(df):
    stats = df.select(
        mean("likes").alias("mean_likes"),
        mean("comments").alias("mean_comments"),
        mean("shares").alias("mean_shares"),
        mean("reach").alias("mean_reach")
    ).first()

    return stats


def impute_metrics(df, stats):
    return df.fillna({
        "likes": stats["mean_likes"],
        "comments": stats["mean_comments"],
        "shares": stats["mean_shares"],
        "reach": stats["mean_reach"]
    })