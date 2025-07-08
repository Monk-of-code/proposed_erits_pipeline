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
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

def AggregatePower():
    """Aggregates power customers to cadastre parcels and writes to temporary summary table"""
    try:
        # Read schema name from environment
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')
        if not schema_name:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting power customer aggregation...")

        # Read base cadastre (lightweight, geometry only)
        logger.info("Loading cadastre base geometry...")
        gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel")

        # Read power customers data
        logger.info("Loading power customers data...")
        gdfPower = UtilDB.readFromDB(schema_name, "langata_power_customers")

        if gdfPower.empty:
            logger.warning("No power customers found - writing empty summary")
            empty_summary = gpd.GeoDataFrame(columns=['index_right', 'power_count'])
            UtilDB.WriteToDB(empty_summary, "tmp_power_summary", "AggregatePower", if_exists="replace")
            return

        # Perform spatial join
        logger.info(f"Performing spatial join on {len(gdfPower)} power customers...")
        joined_gdf = gpd.sjoin(
            gdfPower, gdfCadastre,
            how='inner', predicate='intersects'
        )

        # Aggregate by cadastre polygon (directly naming the column)
        logger.info("Aggregating power customers by parcel...")
        summary = (
            joined_gdf.groupby("index_right")
            .agg(power_count=("index_right", "count"))  # Direct column naming
            .reset_index()
        )

        # Convert to GeoDataFrame by merging with cadastre geometries
        summary = gdfCadastre[['geom']].merge(
            summary,
            left_index=True,
            right_on="index_right",
            how="right"
        )

        logger.info(f"Found {len(summary)} parcels with power customers")

        # Save result to TEMPORARY table
        logger.info("Writing to temporary power summary table...")
        UtilDB.WriteToDB(
            summary,  # Now a proper GeoDataFrame
            "tmp_power_summary",  # Temporary table name
            "AggregatePower",
            {"geometry":"geom"}
        )

        logger.info(f"Power aggregation complete. Total customers: {gdfPower.shape[0]}")

    except Exception as e:
        logger.error(f"Power aggregation failed: {str(e)}", exc_info=True)
        raise