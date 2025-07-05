# debug_spark.py

from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator
import os
from pathlib import Path
from dotenv import load_dotenv

# === Load environment variables from .env ===
BASE_DIR = Path(__file__).resolve().parent
config_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=config_path)

# === SparkSession setup ===
spark = SparkSession.builder \
    .appName("DebugSparkSedona") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,"
            "org.apache.sedona:sedona-viz-3.0_2.12:1.4.1,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

SedonaRegistrator.registerAll(spark)

# === Load DB config ===
host = os.getenv("PIPELINE_HOST_IP")
port = os.getenv("PIPELINE_HOST_PORT")
db = os.getenv("PIPELINE_HOST_NAME")
user = os.getenv("PIPELINE_HOST_USER")
password = os.getenv("PIPELINE_HOST_PASSWORD")
schema = os.getenv("PIPELINE_BRONZE_SCHEMA")

url = f"jdbc:postgresql://{host}:{port}/{db}"
table = f'"{schema}".au_subcounty_unit'  # Preserve case-sensitive schema

# === Attempt to read from PostgreSQL ===
try:
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.show(5, truncate=False)
    print("✅ Spark read from DB successful!")

except Exception as e:
    print(f"❌ Spark DB read failed: {e}")

finally:
    spark.stop()
