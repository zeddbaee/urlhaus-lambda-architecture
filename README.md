# URLHaus Lambda Architecture

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka">
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white" alt="Elasticsearch">
  <img src="https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black" alt="Hadoop">
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker">
</p>

Sistem pemrosesan data ancaman URL secara real-time menggunakan **Lambda Architecture** yang mengintegrasikan **Batch Layer** (HDFS) dan **Speed Layer** (Kafka + Spark Streaming) dengan machine learning untuk klasifikasi malware.

---

## 🏗️ Arsitektur Sistem

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           URLHaus Lambda Architecture                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────────┐   │
│   │  URLHaus    │────▶│    Kafka    │────▶│  Spark Streaming Consumer   │   │
│   │  API        │     │   Broker    │     │  (ML Classification)        │   │
│   └─────────────┘     └─────────────┘     └──────────────┬──────────────┘   │
│                                                          │                   │
│                           ┌──────────────────────────────┼──────────────┐   │
│                           │                              │              │   │
│                           ▼                              ▼              ▼   │
│                  ┌─────────────────┐          ┌──────────────┐  ┌──────────┐│
│                  │      HDFS       │          │Elasticsearch │  │ Telegram ││
│                  │  (Batch Layer)  │          │(Speed Layer) │  │  Alert   ││
│                  └─────────────────┘          └──────┬───────┘  └──────────┘│
│                                                      │                      │
│                                                      ▼                      │
│                                               ┌──────────────┐              │
│                                               │    Kibana    │              │
│                                               │ (Dashboard)  │              │
│                                               └──────────────┘              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🧩 Komponen Utama

| Komponen | Deskripsi | Port |
|----------|-----------|------|
| **Zookeeper** | Koordinasi cluster Kafka | `2181` |
| **Kafka** | Message broker untuk streaming data | `9092` |
| **Kafka UI** | Antarmuka web untuk monitoring Kafka | `8080` |
| **Elasticsearch** | Penyimpanan dan pencarian data (Speed Layer) | `9200` |
| **Kibana** | Visualisasi dan dashboard | `5601` |
| **HDFS NameNode** | Master node untuk penyimpanan batch | `9870`, `9000` |
| **HDFS DataNode** | Worker node untuk penyimpanan batch | - |
| **Producer** | Mengambil data dari URLHaus API ke Kafka | - |
| **Consumer** | Spark Streaming + ML untuk pemrosesan data | - |

---

## ⚙️ Teknologi Stack

- **Message Streaming**: Apache Kafka 7.4.0
- **Stream Processing**: Apache Spark 3.5.0 dengan PySpark
- **Machine Learning**: Spark MLlib (Random Forest Classifier)
- **Batch Storage**: Apache Hadoop HDFS 3.2.1
- **Real-time Storage**: Elasticsearch 7.17.10
- **Visualization**: Kibana 7.17.10
- **Alerting**: Telegram Bot API
- **Container**: Docker & Docker Compose

---

## 🚀 Cara Menjalankan

### Prerequisites

- Docker dan Docker Compose terinstall
- Minimal 8GB RAM tersedia
- Koneksi internet untuk mengakses URLHaus API

### 1. Clone Repository

```bash
git clone https://github.com/username/urlhaus-lambda-architecture.git
cd urlhaus-lambda-architecture
```

### 2. Konfigurasi

Edit file berikut sesuai dengan environment Anda:

**docker-compose.yml**
```yaml
# Ganti [IP_Address] dengan IP host Anda
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://[IP_Address]:9092
```

**producer/urlhaus_producer.py**
```python
KAFKA_SERVER = '[IP_Address]:9092'
```

**consumer/urlhaus_consumer.py**
```python
TELEGRAM_BOT_TOKEN = '[Telegram_Bot_Token]'
TELEGRAM_CHAT_ID = '[Telegram_Chat_ID]'
```

### 3. Jalankan Semua Services

```bash
docker-compose up -d --build
```

### 4. Verifikasi Services

```bash
# Cek status semua container
docker-compose ps

# Lihat log producer
docker-compose logs -f producer

# Lihat log consumer
docker-compose logs -f consumer
```

---

## 📊 Akses Dashboard

| Service | URL |
|---------|-----|
| **Kafka UI** | http://localhost:8080 |
| **Kibana** | http://localhost:5601 |
| **Elasticsearch** | http://localhost:9200 |
| **HDFS NameNode** | http://localhost:9870 |

---

## 🔬 Fitur Machine Learning

Sistem menggunakan **Random Forest Classifier** untuk mengklasifikasikan malware family berdasarkan fitur:

| Fitur | Deskripsi |
|-------|-----------|
| `hour_of_day` | Jam URL ditambahkan |
| `url_length` | Panjang karakter URL |
| `url_complexity` | Rasio karakter spesial terhadap panjang URL |
| `tag_count` | Jumlah tag yang terkait |
| `special_char_count` | Jumlah karakter non-alphanumerik |

### Malware Families yang Dideteksi

- **IoT Botnets**: Mozi, Mirai, Gafgyt
- **Loaders/Info Stealers**: Emotet, AgentTesla, Amadey
- **Post-Exploitation**: Cobaltstrike
- **Others**: Coinminer, Clearfake, ELF

---

## 🔔 Telegram Alerting

Sistem mengirimkan notifikasi real-time ke Telegram dengan informasi:

- Total URL yang diproses per batch
- Top 3 malware family yang terdeteksi
- Rekomendasi tindakan untuk malware dominan

### Contoh Alert

```
🚨 BATCH ANALYSIS 5 🚨
📊 Total URL Diproses: 150

Top Detected Families (Top 3):
🦠 Mirai: 45
🦠 Coinminer: 30
🦠 Emotet: 25

TINDAKAN REKOMENDASI (Untuk Mirai):
**Aksi:** Isolasi dan perbarui firmware perangkat IoT/Router...
```

---

## 📁 Struktur Project

```
urlhaus-lambda-architecture/
├── docker-compose.yml          # Konfigurasi Docker Compose
├── hadoop.env                  # Environment variables untuk Hadoop
├── producer/
│   ├── Dockerfile              # Docker image untuk producer
│   └── urlhaus_producer.py     # Script pengambilan data dari URLHaus API
├── consumer/
│   ├── Dockerfile              # Docker image untuk consumer
│   ├── urlhaus_consumer.py     # Spark Streaming + ML pipeline
│   ├── check_hdfs.py           # Utility untuk cek data di HDFS
│   └── download_hdfs.py        # Utility untuk download data dari HDFS
└── README.md
```

---

## 🛠️ Utility Scripts

### Cek Data di HDFS

```bash
docker exec -it consumer python check_hdfs.py
```

### Download Data dari HDFS

```bash
docker exec -it consumer python download_hdfs.py
```

---

## 📈 Data Flow

1. **Producer** mengambil data ancaman URL terbaru dari [URLHaus API](https://urlhaus.abuse.ch/)
2. Data dikirim ke **Kafka** topic `urlhaus-threats`
3. **Spark Streaming Consumer** membaca dari Kafka setiap 15 detik
4. Data diperkaya dengan feature engineering untuk ML
5. **Random Forest Classifier** memprediksi malware family
6. Data disimpan ke:
   - **HDFS** (Batch Layer) dalam format Parquet
   - **Elasticsearch** (Speed Layer) untuk query real-time
7. **Telegram Alert** dikirim dengan analisis batch

---

## 🛑 Menghentikan Services

```bash
# Hentikan semua services
docker-compose down

# Hentikan dan hapus volumes
docker-compose down -v
```

---

## 📚 Referensi

- [URLHaus API Documentation](https://urlhaus.abuse.ch/api/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Elasticsearch Reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)

---

## 📄 License

Copyright &copy; 2025 Muhammad Riza Zaidaan

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for more information.
