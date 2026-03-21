from pyspark.sql import SparkSession

# creating the Spark session
spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

# reading from Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "transactions").option("startingOffsets", "latest").load()

# converting value to string
df = df.selectExpr("CAST(value AS STRING)")

# printing to the console
query = df.writeStream.format("console").outputMode("append").start()

query.awaitTermination()