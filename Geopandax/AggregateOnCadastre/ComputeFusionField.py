import os
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql import SparkSession
import geopandas as gpd
from pyspark.sql.functions import col
import asyncio
import numpy as np
import logging
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)



def AddMeasureFusionField():
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    gdf_cadastre = UtilDB.readFromDB(
        f"{schema_name}", "langata_cp_power_water_bld_permit_address")

    gdf_cadastre["fusion"] = 0

    gdf_cadastre["fusion"] = np.where(gdf_cadastre["power_count"] > 0,
                                      gdf_cadastre["fusion"]+1,
                                      gdf_cadastre["fusion"])

    gdf_cadastre["fusion"] = np.where(gdf_cadastre["water_count"] > 0,
                                      gdf_cadastre["fusion"]+1,
                                      gdf_cadastre["fusion"])

    gdf_cadastre["fusion"] = np.where(gdf_cadastre["building_count"] > 0,
                                      gdf_cadastre["fusion"]+1,
                                      gdf_cadastre["fusion"])

    gdf_cadastre["fusion"] = np.where(gdf_cadastre["permit_count"] > 0,
                                      gdf_cadastre["fusion"]+1,
                                      gdf_cadastre["fusion"])

    gdf_cadastre["fusion"] = np.where(gdf_cadastre["address_count"] > 0,
                                      gdf_cadastre["fusion"]+1,
                                      gdf_cadastre["fusion"])

    print(f'Cadastre with fusion field: {gdf_cadastre.shape[0]}')

    UtilDB.WriteToDB(
        gdf_cadastre, "langata_cp_power_water_bld_permit_address", "AddMeasureFusionField", "")
