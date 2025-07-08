import logging
import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv

from Geopandax.Utilities import UtilDB

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')


def AddressLocations(selectedSubCounty):
    """Address Locations Preparation Section"""

    # Read address locations
    gdfAddressLocations = UtilDB.readFromDB(schema_name, "address")
    initial_columns = gdfAddressLocations.columns

    # Spatial join: filter by selected subcounty
    joined = gpd.sjoin(gdfAddressLocations, selectedSubCounty, how='inner', predicate='intersects')

    # Drop '_right' columns
    joined.drop(columns=[col for col in joined.columns if col.endswith('_right')], inplace=True)

    # Rename '_left' columns
    joined.rename(columns={col: col.replace('_left', '') for col in joined.columns}, inplace=True)

    # Keep only original address columns
    cleaned = joined[[col for col in joined.columns if col in initial_columns]]

    # Write to DB
    UtilDB.WriteToDB(cleaned, "langata_address_locations", "AddressLocations", "")
