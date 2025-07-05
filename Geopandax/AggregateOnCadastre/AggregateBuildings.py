from datetime import datetime
import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import geopandas as gpd
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator
from Geopandax.Utilities import UtilDB

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

# Initialize Spark with Sedona
spark = SparkSession.builder \
    .appName("BuildingAggregation") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.jars.packages", "org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.4.1,org.apache.sedona:sedona-viz-3.0_2.12:1.4.1") \
    .getOrCreate()

SedonaRegistrator.registerAll(spark)

def AggregateBuildings():
    try:
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

        gdfCadastre = UtilDB.readFromDB(
            f"{schema_name}", "langata_cp_power_water_bld_permit_address")

        now = datetime.now()

        gdfBuildings = UtilDB.readFromDBWithTimestamp(
            f"{schema_name}", "langata_buildings", "2025-02-04 00:00:00")

        if gdfBuildings.empty:
            print("No new building data found.")
            return

        print(f'Number of buildings loaded: {gdfBuildings.shape[0]}')
        print(f'{gdfBuildings.head(2)}')

        joined_gdf = gpd.sjoin(gdfBuildings, gdfCadastre, how='inner',
                               predicate='intersects')

        print(f'joined_gdf: {joined_gdf.shape[0]}')

        summary = (
            joined_gdf.groupby("index_right")
            .agg(point_count=("index_right", "count"))
            .reset_index()
        )

        gdfCadastre_with_summary = gdfCadastre.merge(
            summary,
            left_index=True,
            right_on="index_right",
            how="left"
        )

        gdfCadastre_with_summary.drop(columns=["index_right"], inplace=True)
        gdfCadastre_with_summary.rename(
            columns={"point_count": "building_count"}, inplace=True)

        gdfCadastre_with_summary.reset_index(drop=True, inplace=True)

        print(f'Buildings count: {gdfBuildings.shape[0]}')

        UtilDB.WriteToDB(gdfCadastre_with_summary,
                         "langata_cp_power_water_bld_permit_address", "AggregateBuildings", "")

    except Exception as e:
        print(f"Error in AggregateBuildings: {e}")