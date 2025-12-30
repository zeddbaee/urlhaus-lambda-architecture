from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ExportToCSV").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_PATH = "hdfs://[IP_Address]:9000/user/root/urlhaus_data_parquet/"
LOCAL_OUTPUT = "file:///app/hasil_export_csv"

print("--- Membaca data dari HDFS ---")
df = spark.read.parquet(HDFS_PATH)

print("---- Mengkonversi ke .csv ----")
df_csv = df.withColumn("tags", col("tags").cast("string"))

df_csv.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(LOCAL_OUTPUT)

print(f"[SUKSES] Data tersimpan di folder 'consumer/hasil_export_csv'")