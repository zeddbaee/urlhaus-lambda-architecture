import json
import time
import requests
import logging
from kafka import KafkaProducer

API_KEY = 'f6aab0389911b71a9b142aab9c5c80db547d5774db0b7b18'
KAFKA_SERVER = '[IP_Address]:9092'
KAFKA_TOPIC = 'urlhaus-threats'
API_URL = "https://urlhaus-api.abuse.ch/v1/urls/recent/"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info(f"[INFO] Kafka Ready di {KAFKA_SERVER}")
except Exception as e:
    logging.error(f"[INFO] Kafka Error: {e}")
    exit()


def start_producer():
    logging.info("[INFO] Memulai URLhaus Producer...")

    while True:
        try:
            headers = {'Auth-Key': API_KEY, 'Content-Type': 'application/json'}

            response = requests.get(API_URL, headers=headers, timeout=20)

            if response.status_code == 200:
                data = response.json()
                if data.get('query_status') == 'ok':
                    urls = data.get('urls', [])
                    print(f"\n[INFO] Mengambil {len(urls)} data terbaru. Mulai streaming ke Kafka...")

                    for item in urls:
                        message = {
                            "id": item.get('id'),
                            "url": item.get('url'),
                            "status": item.get('url_status'),
                            "threat": item.get('threat'),
                            "tags": item.get('tags'),
                            "date_added": item.get('date_added'),
                            "reporter": item.get('reporter')
                        }

                        producer.send(KAFKA_TOPIC, message)
                        print(f"   [-> KAFKA] Threat: {message['url']}")

                        time.sleep(0.2)

                else:
                    print(f"   (Query Status: {data.get('query_status')})")
            else:
                logging.warning(f"[INFO] Status API: {response.status_code}")

        except Exception as e:
            logging.error(f"[INFO] Error Koneksi: {e}")
            time.sleep(5)

if __name__ == "__main__":
    start_producer()