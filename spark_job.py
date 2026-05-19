import os
import urllib.request
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, current_timestamp, date_format
print("Начинаем подготовку...")

m2_repo = os.path.expanduser("~/.m2/repository")
if os.path.exists(m2_repo):
    shutil.rmtree(m2_repo, ignore_errors=True)

hadoop_dir = os.path.join(os.getcwd(), "hadoop")
bin_dir = os.path.join(hadoop_dir, "bin")
os.makedirs(bin_dir, exist_ok=True)

winutils_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe"
hadoop_dll_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll"

os.environ['HADOOP_HOME'] = hadoop_dir
os.environ['PATH'] += os.pathsep + bin_dir

print("Запуск Spark...")
clean_ivy_cache = os.path.join(tempfile.gettempdir(), "spark_super_clean_cache")

spark = SparkSession.builder \
    .appName("Taxi_Anti_Rating_Job") \
    .master("local[*]") \
    .config("spark.jars.packages", "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2") \
    .config("spark.jars.ivy", clean_ivy_cache) \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .getOrCreate()

print("Spark успешно запущен!")

DB_URL = "jdbc:clickhouse://127.0.0.1:8123/default"
DB_USER = "default"
DB_PASSWORD = "root"
DRIVER = "ru.yandex.clickhouse.ClickHouseDriver"

print("Загружаем данные из ClickHouse...")
df = spark.read \
    .format("jdbc") \
    .option("url", DB_URL) \
    .option("driver", DRIVER) \
    .option("dbtable", "taxi_trips") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .load()

print("Выполняем вычисления...")
anti_rating_df = df.groupBy("pulocationid") \
    .agg(
        count("*").alias("trips_count"),
        avg("total_amount").alias("avg_amount")
    ) \
    .filter(col("trips_count") > 5) \
    .orderBy(col("avg_amount").asc()) \
    .limit(10)

final_df = anti_rating_df.withColumn("calc_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
final_df.show()

print("Записываем результат в ClickHouse...")
final_df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", DB_URL) \
    .option("driver", DRIVER) \
    .option("dbtable", "taxi_anti_rating") \
    .option("user", DB_USER) \
    .option("password", DB_PASSWORD) \
    .option("isolationLevel", "NONE") \
    .save()

print("Задача успешно завершена!")
spark.stop()