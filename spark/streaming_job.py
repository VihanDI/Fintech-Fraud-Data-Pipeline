from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType

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

# printing to the console
query = parsed_df.writeStream.format("console").option("truncate", "false").outputMode("append").start()

query.awaitTermination()