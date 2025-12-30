from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckHDFS") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

HDFS_PATH = "hdfs://[IP_Address]:9000/user/root/urlhaus_data_parquet/"

print(f"\n{'=' * 50}")
print(f"   MEMERIKSA DATA HADOOP (PARQUET)")
print(f"   Path: {HDFS_PATH}")
print(f"{'=' * 50}\n")

try:
    df = spark.read.parquet(HDFS_PATH)

    print("[INFO] Struktur Data (Schema):")
    df.printSchema()

    total_rows = df.count()
    print(f"[INFO] Total Data Tersimpan: {total_rows} baris")

    print("\n[INFO] Sampel Data (20 Teratas):")
    df.show(20, truncate=False)

except Exception as e:
    print(f"\n[ERROR] Gagal membaca data!")
    print(f"Penyebab: {e}")
    print("\nKemungkinan: Data belum ada (consumer belum menulis) atau path salah.")