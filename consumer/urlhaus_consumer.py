from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import requests
import json
import re

TELEGRAM_BOT_TOKEN = '[Telegram_Bot_Token]'
TELEGRAM_CHAT_ID = '[Telegram_Chat_ID]'

MALWARE_ACTIONS = {
    # IoT Botnets - Mengincar perangkat IoT/Router dengan kredensial default/bug
    'Mozi': "**Aksi:** Segera perbarui firmware dan ubah kredensial default pada perangkat IoT/Router yang terhubung. Blok koneksi keluar SSH/Telnet yang tidak biasa.",
    'Mirai': "**Aksi:** Isolasi dan perbarui firmware perangkat IoT/Router. Nonaktifkan akses Telnet/SSH yang tidak terpakai dari luar jaringan.",
    'Gafgyt': "**Aksi:** Sama seperti Mozi/Mirai. Fokus pada *patching* kerentanan pada perangkat IoT dan pengawasan CPU *load*.",

    # Loaders / Info Stealers - Sering disebar via email, menjadi jembatan ke serangan lain
    'Emotet': "**Aksi:** **Isolasi Host:** Segera isolasi mesin yang terinfeksi. **Email Security:** Terapkan filter email ketat (blok ZIP/EXE), nonaktifkan/batasi makro pada dokumen Office, dan lakukan pelatihan kesadaran phishing.",
    'AgentTesla': "**Aksi:** Gunakan solusi EDR/Antivirus yang kuat. Lakukan audit pada kredensial dan data yang baru-baru ini diakses oleh pengguna yang terinfeksi.",
    'Amadey': "**Aksi:** Sama seperti Emotet/AgentTesla. Prioritaskan *patching* dan *user education* terhadap lampiran email yang mencurigakan.",

    # Post-Exploitation / Spyware
    'Cobaltstrike': "**Aksi:** **Respon Cepat (High Priority):** Lakukan isolasi host yang terdeteksi. Segera periksa tanda-tanda *lateral movement* dan keberadaan malware susulan. Tingkatkan solusi EDR/NDR.",

    # Lain-lain
    'Coinminer': "**Aksi:** Pantau lonjakan penggunaan CPU yang tidak wajar pada server atau endpoint. Cari dan hapus payload *cryptominer* yang terinstal di sistem.",
    'Clearfake': "**Aksi:** Lakukan edukasi pengguna terhadap teknik *social engineering* (notifikasi update palsu) dan *malvertising*. Blok domain yang dicurigai.",
    'Elf': "**Aksi:** Ancaman file *Linux Executable*. Isolasi host Linux yang terinfeksi dan periksa adanya *unauthorized command execution*.",
    'Other': "**Aksi:** Lakukan analisis *forensik* mendalam pada URL. Perkuat kebijakan firewall dan endpoint security.",
    'Unknown': "**Aksi:** Tidak ada tindakan spesifik. Perkuat monitoring umum dan keamanan perimeter."
}

def get_remediation_action(family_name):
    return MALWARE_ACTIONS.get(family_name, MALWARE_ACTIONS['Unknown'])

def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
        requests.post(url, data=data)
    except Exception as e:
        print(f"   [ERROR] Gagal kirim Telegram: {e}")

spark = SparkSession.builder \
    .appName("URLHausConsumer") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("id", StringType()),
    StructField("url", StringType()),
    StructField("status", StringType()),
    StructField("threat", StringType()),
    StructField("tags", ArrayType(StringType())),
    StructField("date_added", StringType()),
    StructField("reporter", StringType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "urlhaus-threats") \
    .option("startingOffsets", "earliest") \
    .load()
df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

def count_special(url):
    import re
    if url: return len(re.sub(r'[a-zA-Z0-9]', '', url))
    return 0

count_special_udf = udf(count_special, IntegerType())

def extract_malware_family(tags):
    if not tags: return "Unknown"
    tags_lower = [t.lower() for t in tags]
    families = ['mozi', 'mirai', 'coinminer', 'clearfake', 'gafgyt', 'cobaltstrike', 'amadey', 'agenttesla', 'emotet']
    for f in families:
        for t in tags_lower:
            if f in t: return f.capitalize()
    types = ['elf', 'exe', 'dll', 'sh', 'botnet', 'backdoor', 'ransomware']
    for ty in types:
        for t in tags_lower:
            if ty in t: return ty.capitalize()
    return "Other"


extract_family_udf = udf(extract_malware_family, StringType())

df_features = df_parsed \
    .withColumn("timestamp", to_timestamp(col("date_added"), "yyyy-MM-dd HH:mm:ss z")) \
    .withColumn("hour_of_day", hour(col("timestamp"))) \
    .withColumn("url_length", length(col("url"))) \
    .withColumn("tag_count", when(col("tags").isNull(), 0).otherwise(size(col("tags")))) \
    .withColumn("special_char_count", count_special_udf(col("url"))) \
    .withColumn("url_complexity", (col("special_char_count") / col("url_length")).cast("float")) \
    .withColumn("malware_family", extract_family_udf(col("tags"))) \
    .fillna(0)


def process_batch(batch_df, batch_id):
    batch_df.persist()
    count = batch_df.count()
    print(f"--- Processing Batch ID: {batch_id} with {count} records ---")

    if count > 0:
        try:
            batch_df.write \
                .mode("append") \
                .parquet("hdfs://[IP_Address]:9000/user/root/urlhaus_data_parquet/")
            print("   [SUCCESS] Data saved to HDFS")
        except Exception as e:
            print(f"   [ERROR] HDFS Write failed: {e}")

        final_df = batch_df
        try:
            indexer = StringIndexer(inputCol="malware_family", outputCol="label").setHandleInvalid("keep")
            indexer_model = indexer.fit(batch_df)
            assembler = VectorAssembler(
                inputCols=["hour_of_day", "url_length", "url_complexity", "tag_count", "special_char_count"],
                outputCol="features",
                handleInvalid="skip"
            )

            unique_labels = batch_df.select("malware_family").distinct().count()

            if unique_labels > 1:
                rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=15, maxDepth=8)
                label_converter = IndexToString(inputCol="prediction", outputCol="predicted_family",
                                                labels=indexer_model.labels)
                pipeline = Pipeline(stages=[indexer, assembler, rf, label_converter])

                model = pipeline.fit(batch_df)
                final_df = model.transform(batch_df)

                print(f"   [ML SUCCESS] Model retrained on {unique_labels} malware families.")
            else:
                final_df = batch_df.withColumn("predicted_family", lit("Unclassified"))

        except Exception as e:
            print(f"   [ML ERROR] Pipeline failed: {e}")
            final_df = batch_df.withColumn("predicted_family", lit("Error"))

        try:
            family_counts = final_df.groupBy("malware_family").count().orderBy(desc("count")).collect()

            msg_lines = [f"🚨 **BATCH ANALYSIS {batch_id}** 🚨"]
            msg_lines.append(f"📊 Total URL Diproses: {count}")

            top_families = [row['malware_family'] for row in family_counts[:3]]

            msg_lines.append("\n**Top Detected Families (Top 3):**")
            for row in family_counts[:3]:
                msg_lines.append(f"🦠 {row['malware_family']}: {row['count']}")

            if family_counts:
                dominant_family = family_counts[0]['malware_family']
                action = get_remediation_action(dominant_family)
                msg_lines.append(f"\n**TINDAKAN REKOMENDASI (Untuk {dominant_family}):**")
                msg_lines.append(action)

            full_msg = "\n".join(msg_lines)
            send_telegram_alert(full_msg)
            print("   [TELEGRAM] Notification sent.")

        except Exception as e:
            print(f"   [TELEGRAM ERROR] Failed: {e}")

        try:
            cols_to_save = [
                "id", "url", "status", "threat", "tags", "reporter",
                "hour_of_day", "url_length", "url_complexity", "tag_count",
                "timestamp", "malware_family"
            ]

            if "predicted_family" in final_df.columns:
                cols_to_save.append("predicted_family")

            final_df.select(*cols_to_save) \
                .write \
                .format("org.elasticsearch.spark.sql") \
                .option("es.resource", "urlhaus_index/_doc") \
                .option("es.nodes", "[IP_Address]") \
                .option("es.port", "9200") \
                .mode("append") \
                .save()
            print("   [SUCCESS] Data sent to Elasticsearch")
        except Exception as e:
            print(f"   [ERROR] Elastic Write failed: {e}")

    batch_df.unpersist()

query = df_features.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .trigger(processingTime='15 seconds') \
    .start()

query.awaitTermination()