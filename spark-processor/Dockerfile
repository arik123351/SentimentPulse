
FROM bitnami/spark:latest

COPY spark_app.py /opt/spark-apps/

RUN pip install textblob

RUN pip install tweepy

CMD ["/opt/bitnami/scripts/spark/entrypoint.sh", "/opt/spark/bin/spark-submit", "/opt/spark-apps/spark_app.py"]