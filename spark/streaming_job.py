from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from typing import Iterator, Tuple
import pandas as pd

def write_to_postgres(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    batch_df.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/airflow").option("dbtable", "transactions").option("user", "airflow").option("password", "airflow").option("driver", "org.postgresql.Driver").mode("append").save()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("merchant_category", ArrayType(StringType()), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True)
])

# creating the Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# reading from Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "transactions").option("startingOffsets", "earliest").load()

# parsing json
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

parsed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))
parsed_df = parsed_df.withColumn("merchant_category", concat_ws(",", col("merchant_category")))

# output schema
output_schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("location", StringType()),
    StructField("fraud_flag", StringType())
])

state_schema = StructType([
    StructField("prev_location", StringType()),
    StructField("prev_time", TimestampType())
])

def detect_fraud(key: Tuple, rows: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
    # handling state timeout
    if state.hasTimedOut:
        state.remove()
        return

    state.setTimeoutDuration(600000)

    # retrieving the previous state
    if state.exists:
        prev_state = state.get
        prev_location = prev_state[0]
        prev_time = prev_state[1]
    else:
        prev_location, prev_time = None, None

    for batch in rows:
        results = []

        for _, row in batch.iterrows():
            fraud_flag = "NORMAL"

            if row["amount"] > 5000:
                fraud_flag = "FRAUD"

            if prev_location and prev_time:
                time_diff = (row["timestamp"] - prev_time).total_seconds() / 60
                if row["location"] != prev_location and time_diff <= 10:
                    fraud_flag = "FRAUD"

            # only update state from legitimate transactions
            if fraud_flag == "NORMAL":
                prev_location = row["location"]
                prev_time = row["timestamp"]

            results.append({
                "user_id": row["user_id"],
                "timestamp": row["timestamp"],
                "merchant_category": row["merchant_category"],
                "amount": row["amount"],
                "location": row["location"],
                "fraud_flag": fraud_flag
            })

        # updating the state
        state.update((prev_location, prev_time))

        yield pd.DataFrame(results)

# appling stateful processing
stateful_df = parsed_df.groupBy("user_id").applyInPandasWithState(
    func=detect_fraud,
    outputStructType=output_schema,
    stateStructType=state_schema,
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)

# writing to the database
query = stateful_df.writeStream.foreachBatch(write_to_postgres).outputMode("append").option("checkpointLocation", "/tmp/spark-checkpoints/transactions").trigger(processingTime="10 seconds").start()

query.awaitTermination()