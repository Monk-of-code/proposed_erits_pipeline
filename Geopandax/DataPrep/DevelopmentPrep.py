import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')
schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')


def DevelopmentControl(selectedSubCounty):
    """Development Control Preparation Section"""

    gdfDC = UtilDB.readFromDB(schema_name, "building_permits")
    initial_columns = gdfDC.columns

    # Spatial join with subcounty
    joined = gpd.sjoin(gdfDC, selectedSubCounty, how='inner', predicate='intersects')

    # Drop '_right' columns
    joined.drop(columns=[col for col in joined.columns if col.endswith('_right')], inplace=True)

    # Rename '_left' columns
    joined.rename(columns={col: col.replace('_left', '') for col in joined.columns}, inplace=True)

    # Keep only original columns
    cleaned = joined[[col for col in joined.columns if col in initial_columns]]

    # Write to DB
    UtilDB.WriteToDB(cleaned, "langata_building_permits", "DevelopmentControl", "")
