import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')
schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')


def Power(selectedSubCounty):
    """Power Customers Preparation Section"""

    gdfPower = UtilDB.readFromDB(schema_name, "power_customers")
    initial_columns = gdfPower.columns

    # Spatial join with selected subcounty
    joined = gpd.sjoin(gdfPower, selectedSubCounty, how='inner', predicate='intersects')

    # Drop '_right' columns
    joined.drop(columns=[col for col in joined.columns if col.endswith('_right')], inplace=True)

    # Rename '_left' columns
    joined.rename(columns={col: col.replace('_left', '') for col in joined.columns}, inplace=True)

    # Retain only original columns
    cleaned = joined[[col for col in joined.columns if col in initial_columns]]

    # Write cleaned data to DB
    UtilDB.WriteToDB(cleaned, "langata_power_customers", "Power", "")
