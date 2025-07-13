
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType
from textblob import TextBlob

def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

spark = SparkSession.builder \
    .appName("TwitterSentiment") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

schema = StructType() \
    .add("ticker", StringType()) \
    .add("text", StringType())

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-tweets") \
    .load()

df = raw_df.selectExpr("CAST(value AS STRING)")
df_parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

sentiment_udf = udf(get_sentiment)
df_sentiment = df_parsed.withColumn("sentiment", sentiment_udf(col("text")))

query = df_sentiment.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("es.resource", "tweets/_doc") \
    .outputMode("append") \
    .start()

query.awaitTermination()