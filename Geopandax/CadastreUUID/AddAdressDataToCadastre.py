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
    gdfDataset = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permits_address_uuid")

    logger.info(f"[AddAddressPropertyNameToCadastre] Loaded address ({len(address)}) and cadastre ({len(gdfDataset)})")

    initial_columns = gdfDataset.columns.tolist()
    logger.info(f"Initial cadastre columns: {initial_columns}")

    joined_gdf = gpd.sjoin(gdfDataset, address, how='left', predicate='intersects')

    # Drop unnecessary columns
    joined_gdf = joined_gdf.drop(columns=[col for col in joined_gdf.columns if col.endswith('_right')])

    # Fix: Rename columns properly
    joined_gdf = joined_gdf.rename(columns={col: col.replace("_left", "") for col in joined_gdf.columns})

    # Debug: See what columns exist post-join
    logger.info(f"Post-join columns: {joined_gdf.columns.tolist()}")

    if 'propertyna' not in joined_gdf.columns:
        logger.warning("[AddAddressPropertyNameToCadastre] 'propertyna' column not found after join.")
        return

    # Keep only relevant columns
    columns_to_keep = list(set(initial_columns + ['propertyna']))
    gdf_cleaned = joined_gdf[columns_to_keep]

    # Final rename
    gdf_cleaned = gdf_cleaned.rename(columns={'propertyna': "property_name"})

    logger.info(f"Final columns for write: {gdf_cleaned.columns.tolist()}")

    UtilDB.WriteToDB(gdf_cleaned,
                     "langata_cp_power_water_bld_permit_address_uuid",
                     "AddAddressPropertyNameToCadastre",toPostGISOption=False)
