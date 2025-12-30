FROM python:3.9-slim

RUN pip install requests kafka-python

COPY urlhaus_producer.py /app/urlhaus_producer.py

WORKDIR /app
CMD ["python", "urlhaus_producer.py"]