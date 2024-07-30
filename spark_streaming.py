from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               StructField, StructType)

# Define the schema of the transaction data
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("is_fraudulent", BooleanType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransactionMonitoring") \
    .getOrCreate()

# Read from Kafka
transactions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON data
transactions_df = transactions_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Here you can add fraud detection logic and any other processing
# For example, filter out fraudulent transactions
fraudulent_transactions_df = transactions_df.filter(col("is_fraudulent") == True)

# Write the output to the console for debugging
query = fraudulent_transactions_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()