FROM apache/spark:3.5.0

USER root

RUN pip install pandas numpy requests

COPY urlhaus_consumer.py /opt/spark/work-dir/urlhaus_consumer.py

WORKDIR /opt/spark/work-dir

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.10", \
     "urlhaus_consumer.py"]