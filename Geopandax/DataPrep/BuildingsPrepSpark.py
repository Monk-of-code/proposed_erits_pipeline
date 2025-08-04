import os
import warnings
import logging
from pathlib import Path
from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
from sedona.spark import SedonaContext
from sedona.register import SedonaRegistrator  # Keep this for backward compatibility

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

# Load environment variables
BASE_DIR = Path(__file__).resolve().parents[2]
# Try to load .env file, fallback to sample.env if .env doesn't exist
env_file = BASE_DIR / '.env'
if not env_file.exists():
    env_file = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_file)

# Batch configuration
BATCH_SIZE = 100000
TOTAL_RECORDS = 17000000  # 17 million records

def initialize_spark():
    conf = SparkConf()

    # Read environment variables with defaults
    driver_host = os.environ.get("SPARK_DRIVER_HOST", "localhost")
    bind_address = os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0")
    executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY", "4g")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
    serializer = os.environ.get("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
    jars_packages = os.environ.get("SPARK_JARS_PACKAGES", "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.2,org.datasyslab:geotools-wrapper:1.5.1-28.2,org.postgresql:postgresql:42.6.0")
    legacy_udf = os.environ.get("SPARK_LEGACY_UDF", "true")

    # Apply config
    conf.set("spark.driver.host", driver_host)
    conf.set("spark.driver.bindAddress", bind_address)
    conf.set("spark.executor.memory", executor_memory)
    conf.set("spark.driver.memory", driver_memory)
    conf.set("spark.serializer", serializer)
    conf.set("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")
    conf.set("sedona.join.numpartition", "10")
    conf.set("sedona.global.charset", "utf8")

    if legacy_udf.lower() == "true":
        conf.set("sedona.udf.legacy", "true")

    if jars_packages:
        conf.set("spark.jars.packages", jars_packages)

    print(f"Initializing Spark with:")
    print(f"- Driver Host: {driver_host}")
    print(f"- Bind Address: {bind_address}")
    print(f"- Executor Memory: {executor_memory}")
    print(f"- Driver Memory: {driver_memory}")
    print(f"- Serializer: {serializer}")
    print(f"- Jars Packages: {jars_packages}")
    print(f"- Legacy UDF: {legacy_udf}")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    SedonaRegistrator.registerAll(spark)
    return spark

def read_table_batch(spark, db_url, db_user, db_password, schema, table, offset, limit):
    """Read a batch of records from the database"""
    query = f"""
    SELECT * FROM "{schema}"."{table}" 
    ORDER BY id 
    LIMIT {limit} OFFSET {offset}
    """
    
    return spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", f'({query}) as batch_query') \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .na.drop()

def process_batch(spark, buildings_batch_df, subcounty_df, batch_num, total_batches):
    """Process a single batch of buildings data"""
    logger.info(f"Processing batch {batch_num}/{total_batches} with {buildings_batch_df.count()} records")
    
    # Create temporary views for this batch
    buildings_batch_df.createOrReplaceTempView(f"buildings_batch_{batch_num}")
    subcounty_df.createOrReplaceTempView("subcounty")
    
    query = f"""
    SELECT 
        b.*, 
        s.subcounty_name
    FROM buildings_batch_{batch_num} b
    LEFT JOIN subcounty s
    ON ST_Intersects(
        ST_GeomFromWKT(b.geometry),
        ST_GeomFromWKT(s.geometry)
    )
    """
    
    joined_df = spark.sql(query)
    logger.info(f"Batch {batch_num} join result: {joined_df.count()} rows")
    
    return joined_df

def write_batch_to_db(joined_df, db_url, db_user, db_password, silver_schema, batch_num):
    """Write a batch result to the database"""
    table_name = f"langatas_buildings_batch_{batch_num}"
    
    joined_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", f"{silver_schema}.{table_name}") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    logger.info(f"Saved batch {batch_num} to {silver_schema}.{table_name}")

def get_total_count(spark, db_url, db_user, db_password, schema, table):
    """Get total count of records in the table"""
    count_query = f'SELECT COUNT(*) as total FROM "{schema}"."{table}"'
    
    count_df = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", f'({count_query}) as count_query') \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    return count_df.first()["total"]

def combine_batch_results(spark, db_url, db_user, db_password, silver_schema, total_batches):
    """Combine all batch results into a single table"""
    logger.info("Combining all batch results into a single table...")
    
    # Create a union of all batch tables
    union_query = " UNION ALL ".join([
        f'SELECT * FROM "{silver_schema}"."langatas_buildings_batch_{i + 1}"'
        for i in range(total_batches)
    ])
    
    combined_df = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", f'({union_query}) as combined_query') \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    # Write to final table
    combined_df.write \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", f"{silver_schema}.langatas_buildings") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    logger.info(f"Combined {combined_df.count()} records into {silver_schema}.langatas_buildings")

def building_spatial_join():
    spark = initialize_spark()
    SedonaContext.create(spark)

    try:
        # Check if required environment variables are set
        required_env_vars = [
            "PIPELINE_HOST_IP", "PIPELINE_HOST_PORT", "PIPELINE_HOST_NAME",
            "PIPELINE_HOST_USER", "PIPELINE_HOST_PASSWORD",
            "PIPELINE_BRONZE_SCHEMA", "PIPELINE_SILVER_SCHEMA"
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        db_url = f"jdbc:postgresql://{os.getenv('PIPELINE_HOST_IP')}:{os.getenv('PIPELINE_HOST_PORT')}/{os.getenv('PIPELINE_HOST_NAME')}"
        db_user = os.getenv("PIPELINE_HOST_USER")
        db_password = os.getenv("PIPELINE_HOST_PASSWORD")
        bronze_schema = os.getenv("PIPELINE_BRONZE_SCHEMA")
        silver_schema = os.getenv("PIPELINE_SILVER_SCHEMA")

        # Get total count of buildings records
        logger.info("Getting total count of buildings records...")
        total_buildings = get_total_count(spark, db_url, db_user, db_password, bronze_schema, "183_buildings")
        logger.info(f"Total buildings records: {total_buildings}")

        # Load subcounty data once (it's smaller)
        logger.info("Loading subcounty data...")
        subcounty_df = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", f'"{silver_schema}"."langata_au_subcounty_unit"') \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .na.drop()
        
        logger.info(f"Loaded subcounty polygons: {subcounty_df.count()} rows")

        # Calculate number of batches
        total_batches = (total_buildings + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Processing {total_buildings} records in {total_batches} batches of {BATCH_SIZE}")

        # Track successful batches
        successful_batches = []

        # Process batches
        for batch_num in range(total_batches):
            try:
                offset = batch_num * BATCH_SIZE
                
                logger.info(f"Starting batch {batch_num + 1}/{total_batches} (offset: {offset})")
                
                # Read batch of buildings
                buildings_batch_df = read_table_batch(
                    spark, db_url, db_user, db_password, 
                    bronze_schema, "183_buildings", offset, BATCH_SIZE
                )
                
                if buildings_batch_df.count() == 0:
                    logger.warning(f"No records found in batch {batch_num + 1}, skipping...")
                    continue
                
                # Process the batch
                joined_df = process_batch(spark, buildings_batch_df, subcounty_df, batch_num + 1, total_batches)
                
                # Write batch result
                write_batch_to_db(joined_df, db_url, db_user, db_password, silver_schema, batch_num + 1)
                
                successful_batches.append(batch_num + 1)
                logger.info(f"Completed batch {batch_num + 1}/{total_batches}")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_num + 1}: {str(e)}")
                # Continue with next batch instead of failing completely
                continue

        logger.info(f"Processing completed. {len(successful_batches)}/{total_batches} batches successful")
        
        # Combine all successful batches into a single table
        if successful_batches:
            logger.info("Combining successful batches into final table...")
            combine_batch_results(spark, db_url, db_user, db_password, silver_schema, len(successful_batches))
        else:
            logger.warning("No successful batches to combine!")

    except Exception as e:
        logger.error(f"Spatial join failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    building_spatial_join()
