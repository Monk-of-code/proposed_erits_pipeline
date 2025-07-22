from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator  # Keep this for backward compatibility
from sedona.utils import SedonaKryoRegistrator
from sedona.spark import SedonaContext  # New recommended approach
from dotenv import load_dotenv
import os
from pathlib import Path
import logging

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

# Initialize Spark with correct versions for your environment
spark = SparkSession.builder \
    .appName("BuildingAnalysis") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.jars.packages", ",".join([
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.2",
        "org.datasyslab:geotools-wrapper:1.5.1-28.2",
        "org.postgresql:postgresql:42.6.0"
    ])) \
    .config("spark.driver.host", "192.168.101.114") \
    .config("spark.driver.bindAddress", "192.168.101.114") \
    .config("spark.sql.legacy.allowUntypedScalaUDF", "true") \
    .getOrCreate()

sedona = SedonaContext.create(spark)
logger.info("Successfully initialized Spark  and Sedona ")
# Get DB connection details (unchanged)
def get_db_props():
    return {
        "user": os.getenv("PIPELINE_HOST_USER"),
        "password": os.getenv("PIPELINE_HOST_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "url": f"jdbc:postgresql://{os.getenv('PIPELINE_HOST_IP')}:{os.getenv('PIPELINE_HOST_PORT')}/{os.getenv('PIPELINE_HOST_NAME')}"
    }

# Read function (unchanged)
def readFromDB(schema_name, table_name):
    """Read data from PostgreSQL/PostGIS into Spark"""
    db_props = get_db_props()
    full_table = f"{schema_name}.{table_name}"
    df = spark.read.jdbc(
        url=db_props["url"],
        table=full_table,
        properties=db_props
    )
    logger.info(f"Loaded {df.count()} records from {full_table}")
    return df

# Write function (unchanged)
def writeToDB(schema_name, dataFrame, outPutTable, functionName):
    """Write Spark DataFrame back to PostgreSQL"""
    db_props = get_db_props()
    full_table = f"{schema_name}.{outPutTable}"
    dataFrame.write.jdbc(
        url=db_props["url"],
        table=full_table,
        mode="overwrite",
        properties=db_props
    )
    logger.info(f"{functionName} wrote {dataFrame.count()} records to {full_table}")