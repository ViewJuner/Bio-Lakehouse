"""Optional PySpark Structured Streaming job (same contract as stream_processor.py).

Default docker demo runs stream_processor.py so the stack starts without
downloading Spark/Iceberg JARs. To submit on a Spark 3.5 cluster:

  spark-submit --packages \\
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\\
    org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \\
    stream_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, max as smax, min as smin, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("patient_id", StringType()),
        StructField("device_id", StringType()),
        StructField("ts", StringType()),
        StructField("heart_rate", DoubleType()),
        StructField("spo2", DoubleType()),
        StructField("systolic_bp", DoubleType()),
        StructField("diastolic_bp", DoubleType()),
        StructField("temperature", DoubleType()),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("bio-lakehouse-vitals")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "vitals.validated")
        .load()
    )
    parsed = raw.select(from_json(col("value").cast("string"), SCHEMA).alias("v")).select("v.*")
    bronze = parsed.writeStream.format("iceberg").option("checkpointLocation", "/tmp/ck-bronze").toTable(
        "bronze.vitals_raw"
    )
    silver = parsed.filter(
        (col("heart_rate").between(30, 220)) & (col("spo2").between(50, 100))
    )
    gold = silver.groupBy(col("patient_id"), window(col("ts").cast("timestamp"), "1 minute")).agg(
        avg("heart_rate").alias("avg_hr"),
        smax("heart_rate").alias("max_hr"),
        smin("spo2").alias("min_spo2"),
        avg("systolic_bp").alias("avg_sbp"),
    )
    silver.writeStream.format("iceberg").option("checkpointLocation", "/tmp/ck-silver").toTable("silver.vitals_clean")
    gold.writeStream.format("iceberg").option("checkpointLocation", "/tmp/ck-gold").toTable("gold.vitals_agg")
    spark.streams.awaitAnyTermination()
    bronze.stop()


if __name__ == "__main__":
    main()
