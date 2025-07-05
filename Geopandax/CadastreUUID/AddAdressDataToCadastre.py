import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
import logging
from Geopandax.Utilities import UtilDB

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

def AddAddressPropertyNameToCadastre():
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    address = UtilDB.readFromDB(schema_name, "langata_address_locations")
    gdfDataset = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permit_address_uuid")

    initial_columns = gdfDataset.columns

    joined_gdf = gpd.sjoin(gdfDataset, address, how='left', predicate='intersects')

    # Drop columns ending with '_right'
    joined_gdf = joined_gdf.drop(columns=[col for col in joined_gdf.columns if col.endswith('_right')])

    # Rename columns by stripping '_left'
    joined_gdf.rename(columns={col: col.replace("_left", "") for col in joined_gdf.columns}, inplace=True)

    # Keep only required columns
    columns_to_keep = list(initial_columns) + ['propertyna']
    gdf_cleaned = joined_gdf[[col for col in joined_gdf.columns if col in columns_to_keep]]

    # Rename for final schema
    gdf_cleaned.rename(columns={'propertyna': 'propertyName'}, inplace=True)

    UtilDB.WriteToDB(gdf_cleaned,
                     "langata_cp_power_water_bld_permit_address_uuid",
                     "AddAddressPropertyNameToCadastre", "")

    print(f'Cadastre with Address Property Name processed: {gdf_cleaned.shape[0]}')
