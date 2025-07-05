import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext  # Updated import
import warnings
import logging

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", ".*UDTRegistration.*")
warnings.filterwarnings("ignore", ".*SimpleFunctionRegistry.*")

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def initialize_spark():
    """Initialize Spark with configuration from .env"""
    return SparkSession.builder \
        .appName("BuildingAnalysis") \
        .config("spark.jars.packages", os.getenv("SPARK_JARS_PACKAGES")) \
        .config("spark.serializer", os.getenv("SPARK_SERIALIZER")) \
        .config("spark.sql.legacy.allowUntypedScalaUDF", os.getenv("SPARK_LEGACY_UDF")) \
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST")) \
        .config("spark.driver.bindAddress", os.getenv("SPARK_DRIVER_BIND_ADDRESS")) \
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY")) \
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY")) \
        .getOrCreate()

# Initialize Sedona
spark = initialize_spark()
sedona = SedonaContext.create(spark)
def Building(selectedSubCounty):
    """Building Preparation Section using Spark & Sedona"""
    spark = initialize_spark()
    
    try:
        schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')

        # Read building data
        buildings_df = spark.read \
            .format("jdbc") \
            .option("dbtable", f"{schema_name}.langata_buildings") \
            .load()
        buildings_df = buildings_df.na.drop()
        logger.info(f"Loaded buildings data: {buildings_df.count()} rows")

        # Prepare subcounty geometry
        subcounty_df = spark.createDataFrame(
            [(selectedSubCounty,)], ["geometry"]
        ).withColumnRenamed("geometry", "subcounty_geom")
        subcounty_df = subcounty_df.na.drop()

        # Register temporary views
        buildings_df.createOrReplaceTempView("buildings")
        subcounty_df.createOrReplaceTempView("subcounty")

        # Spatial join using Sedona SQL
        query = """
        SELECT b.*
        FROM buildings b
        JOIN subcounty s
        WHERE ST_Intersects(
            ST_GeomFromWKT(b.geometry),
            ST_GeomFromWKT(s.subcounty_geom)
        )
        """
        joined_df = spark.sql(query)
        final_df = joined_df.select(buildings_df.columns)

        # Write to DB
        final_df.write \
            .format("jdbc") \
            .option("dbtable", f"{schema_name}.langata_buildings_spark") \
            .mode("overwrite") \
            .save()
        logger.info(f"Successfully wrote {final_df.count()} buildings to database")

    except Exception as e:
        logger.error(f"Error processing buildings: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")