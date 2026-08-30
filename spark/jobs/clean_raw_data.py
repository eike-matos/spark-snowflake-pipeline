from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Sentiment140 has no header row, uses latin-1 encoding, and columns are:
# target, id, date, flag, user, text
SCHEMA = StructType([
    StructField("target", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("user", StringType(), True),
    StructField("text", StringType(), True),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("sentiment140-cleaning")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def load_raw_data(spark: SparkSession, input_path: str):
    return (
        spark.read
        .option("header", "false")
        .option("encoding", "ISO-8859-1")  # latin-1
        .schema(SCHEMA)
        .csv(input_path)
    )


def clean_data(df):
    return (
        df
        # map target: 0 = negative, 4 = positive (dataset has no neutral=2 rows in practice)
        .withColumn(
            "sentiment",
            F.when(F.col("target") == 0, "negative")
             .when(F.col("target") == 4, "positive")
             .otherwise("unknown")
        )
        # parse the date column into a proper timestamp
        .withColumn("created_at", F.to_timestamp("date", "EEE MMM dd HH:mm:ss zzz yyyy"))
        # clean the tweet text: remove URLs, mentions, extra whitespace
        .withColumn("clean_text", F.regexp_replace(F.col("text"), r"http\S+|www\S+", ""))
        .withColumn("clean_text", F.regexp_replace(F.col("clean_text"), r"@\w+", ""))
        .withColumn("clean_text", F.regexp_replace(F.col("clean_text"), r"\s+", " "))
        .withColumn("clean_text", F.trim(F.col("clean_text")))
        # drop rows with empty text after cleaning
        .filter(F.length(F.col("clean_text")) > 0)
        # drop exact duplicate tweets
        .dropDuplicates(["id"])
        # keep only the columns we actually need downstream
        .select(
            "id",
            "user",
            "created_at",
            "sentiment",
            "clean_text",
        )
    )


def main():
    spark = build_spark_session()

    input_path = "data/training.1600000.processed.noemoticon.csv"
    output_path = "data/staging/tweets_clean"

    raw_df = load_raw_data(spark, input_path)
    print(f"Raw row count: {raw_df.count()}")

    clean_df = clean_data(raw_df)
    print(f"Clean row count: {clean_df.count()}")

    clean_df.show(10, truncate=80)

    clean_df.write.mode("overwrite").parquet(output_path)
    print(f"Written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()