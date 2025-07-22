import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
import logging

from Geopandax.Utilities import UtilDB

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')


def Building(selectedSubCounty):
    """Building Preparation Section"""
    # Read buildings from DB
    gdfBuildings = UtilDB.readFromDB(schema_name, "langata_buildings")
    initial_columns = gdfBuildings.columns
    print(initial_columns)
    # Spatial join with subcounty
    joined = gpd.sjoin(gdfBuildings, selectedSubCounty, how='inner', predicate='intersects')
    logging.info("Spatial join done")
    

# Log and print the columns
    logging.info(f"Columns after spatial join: {joined.columns.tolist()}")

    # Drop '_right' columns
    joined.drop(columns=[col for col in joined.columns if col.endswith('_right')], inplace=True)

    # Rename '_left' columns
    joined.rename(columns={col: col.replace('_left', '') for col in joined.columns}, inplace=True)

    # Drop any columns not in the original
    cleaned = joined[[col for col in joined.columns if col in initial_columns]]
    logging.info(f"Cleaned columns:{cleaned.columns.tolist()}")
       
    # Write cleaned data to DB
    UtilDB.WriteToDB(cleaned, "langata_buildings", "Building", "")
   
