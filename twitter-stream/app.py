import tweepy
from kafka import KafkaProducer
import json
from datetime import datetime

# Twitter credentials (replace with your actual values)
BEARER_TOKEN = "YOUR_TWITTER_BEARER_TOKEN"

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ticker keywords
ticker_keywords = {
    "AAPL": ["Apple", "AAPL"],
    "TSLA": ["Tesla", "TSLA"],
    "GOOGL": ["Google", "Alphabet", "GOOGL"],
    "NVDA": ["Nvidia", "NVDA"],
    "MSFT": ["Microsoft", "MSFT"]
}

# Ticker detection
def extract_ticker(text):
    for ticker, keywords in ticker_keywords.items():
        if any(keyword.lower() in text.lower() for keyword in keywords):
            return ticker
    return "UNKNOWN"

# Define stream listener using Tweepy StreamingClient
class TweetStreamer(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        text = tweet.text
        ticker = extract_ticker(text)
        if ticker != "UNKNOWN":
            message = {
                "ticker": ticker,
                "text": text,
                "@timestamp": datetime.utcnow().isoformat()
            }
            print("Sending:", message)
            producer.send('stock-tweets', message)

# Start streaming
stream = TweetStreamer(BEARER_TOKEN)

# Set up stream rules (add rules only once or check for duplicates)
stream.add_rules(tweepy.StreamRule("Apple OR Tesla OR Nvidia OR Microsoft OR Google"))

print("🟢 Streaming tweets from Twitter API...")
stream.filter(tweet_fields=["text"])
