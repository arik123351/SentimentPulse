from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulated tweets from known financial accounts
import random

keywords = [
    ("AAPL", ["Apple", "iPhone", "Tim Cook"]),
    ("TSLA", ["Tesla", "Elon Musk", "Cybertruck"]),
    ("GOOGL", ["Google", "Alphabet", "Android"]),
    ("NVDA", ["Nvidia", "GPU", "AI"]),
    ("MSFT", ["Microsoft", "Windows", "Azure"])
]

texts = []
for ticker, words in keywords:
    for _ in range(2):
        keyword = random.choice(words)
        texts.append(f"@financeguru: Big moves coming for {keyword}! ${ticker}")

# Infer ticker from message if it contains known symbols
def extract_ticker(text):
    for ticker in ["AAPL", "TSLA", "GOOGL", "NVDA", "MSFT"]:
        if ticker in text:
            return ticker
    return "UNKNOWN"

while True:
    for text in texts:
        tweet = {"ticker": extract_ticker(text), "text": text}
        print("Sending:", tweet)
        producer.send('stock-tweets', tweet)
        time.sleep(3)