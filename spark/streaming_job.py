from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

def write_to_postgres(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    batch_df.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/airflow").option("dbtable", "transactions").option("user", "airflow").option("password", "airflow").option("driver", "org.postgresql.Driver").mode("append").save()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("country", StringType(), True),
    StructField("category", ArrayType(StringType()), True)
])

# creating the Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# reading from Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "transactions").option("startingOffsets", "earliest").load()

# parsing json
# df = df.selectExpr("CAST(value AS STRING)")
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# adding fraud detection rule
fraud_df = parsed_df.withColumn("fraud_flag", when(col("amount") > 3000, "high_value").otherwise("normal"))

fraud_df = fraud_df.withColumn("category", concat_ws(",", col("category")))

# printing to the console
# query = fraud_df.writeStream.format("console").option("truncate", "false").outputMode("append").start()

# writing to the database
# query = fraud_df.writeStream.foreachBatch(write_to_postgres).outputMode("append").start()
query = fraud_df.writeStream.foreachBatch(write_to_postgres).outputMode("append").option("checkpointLocation", "/tmp/spark-checkpoints/transactions").trigger(processingTime="10 seconds").start()

query.awaitTermination()