from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType

# Define schema of incoming JSON messages
schema = StructType() \
    .add("ticker", StringType()) \
    .add("text", StringType())

# Read stream from Kafka
raw_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-tweets") \
    .load()

# Convert the binary 'value' column to string
tweets_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON string to columns
json_df = tweets_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Optionally add sentiment column here (e.g., TextBlob or other)

# For example, just write the original fields for now
output_df = json_df.select("ticker", "text")

# Write to Elasticsearch
es_write_conf = {
    "es.nodes": "elasticsearch",
    "es.port": "9200",
    "es.resource": "tweets/_doc",
    "es.mapping.id": "id"  # optional
}

query = output_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .options(**es_write_conf) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()
