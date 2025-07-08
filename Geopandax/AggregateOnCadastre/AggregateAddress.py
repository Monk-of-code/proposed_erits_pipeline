import os
from pathlib import Path
from dotenv import load_dotenv
import geopandas as gpd
import logging
from Geopandax.Utilities import UtilDB

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

def AggregateAddressPoints():
    """Aggregates address points to cadastre parcels and writes to temporary summary table"""
    try:
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')
        if not schema_name:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting address point aggregation...")

        # Read base cadastre (
        logger.info("Loading cadastre")
        gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel")

        # Read address data
        logger.info("Loading address locations...")
        gdfAddress = UtilDB.readFromDB(schema_name, "langata_address_locations")

        if gdfAddress.empty:
            logger.warning("No address points found - writing empty summary")
            empty_summary = gpd.GeoDataFrame(columns=['index_right', 'address_count'])
            UtilDB.WriteToDB(empty_summary, "tmp_address_summary", "AggregateAddressPoints", if_exists="replace")
            return

        # Perform spatial join
        logger.info(f"Performing spatial join on {len(gdfAddress)} addresses...")
        joined_gdf = gpd.sjoin(
            gdfAddress, gdfCadastre,
            how='inner', predicate='intersects'
        )

        # Aggregate by cadastre polygon (directly naming the column)
        logger.info("Aggregating addresses by parcel...")
        summary = (
            joined_gdf.groupby("index_right")
            .agg(address_count=("index_right", "count"))  # Direct column naming
            .reset_index()
        )

        # Convert to GeoDataFrame by merging with cadastre geometries
        summary = gdfCadastre[['geom']].merge(
            summary,
            left_index=True,
            right_on="index_right",
            how="right"
        )

        logger.info(f"Found {len(summary)} parcels with addresses")

        # Save result to TEMPORARY table
        logger.info("Writing to temporary address summary table...")
        UtilDB.WriteToDB(
            summary,  # Now a proper GeoDataFrame
            "tmp_address_summary",  # Temporary table name
            "AggregateAddressPoints",
            {"geometry":"geom"}  # Overwrite existing
        )

        logger.info(f"Address aggregation complete. Total addresses: {gdfAddress.shape[0]}")

    except Exception as e:
        logger.error(f"Address aggregation failed: {str(e)}", exc_info=True)
        raise