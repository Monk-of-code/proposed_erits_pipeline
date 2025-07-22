from pathlib import Path
import os
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')
schema_name = os.getenv('PIPELINE_BRONZE_SCHEMA')


def Cadastre(selectedSubCounty):
    """Cadastre Preparation Section"""

    gdfCadastre = UtilDB.readFromDB(schema_name, "cp_cadastral_parcel")
    initial_columns = gdfCadastre.columns

    # Spatial join with selected subcounty
    joined = gpd.sjoin(gdfCadastre, selectedSubCounty, how='inner', predicate='intersects')

    # Keep only active parcels
    active = joined[joined['current_status'] == 'Active']

    # Drop '_right' columns
    active.drop(columns=[col for col in active.columns if col.endswith('_right')], inplace=True)

    # Rename '_left' columns
    active.rename(columns={col: col.replace('_left', '') for col in active.columns}, inplace=True)

    # Keep only the original columns
    cleaned = active[[col for col in active.columns if col in initial_columns]]

    # Write to DB
    UtilDB.WriteToDB(cleaned, "langata_cp_cadastral_parcel", "Cadastre", "")
