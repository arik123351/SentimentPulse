from kafka import KafkaProducer
import json
import time
import random

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Keywords associated with each ticker
keywords = [
    ("AAPL", ["Apple", "iPhone", "Tim Cook"]),
    ("TSLA", ["Tesla", "Elon Musk", "Cybertruck"]),
    ("GOOGL", ["Google", "Alphabet", "Android"]),
    ("NVDA", ["Nvidia", "GPU", "AI"]),
    ("MSFT", ["Microsoft", "Windows", "Azure"])
]

# Generate example tweets dynamically
texts = []
for ticker, words in keywords:
    for _ in range(2):
        keyword = random.choice(words)
        texts.append(f"@financeguru: Big moves coming for {keyword}! ${ticker}")

# Function to extract ticker from message
def extract_ticker(text):
    for ticker, _ in keywords:
        if ticker in text:
            return ticker
    return "UNKNOWN"

# Continuously send tweets to Kafka
while True:
    for text in texts:
        tweet = {
            "ticker": extract_ticker(text),
            "text": text
        }
        print("Sending:", tweet)
        producer.send('stock-tweets', tweet)
        time.sleep(3)
