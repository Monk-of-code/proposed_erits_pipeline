import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import geopandas as gpd
from Geopandax.Utilities import UtilDB

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

def AggregateWater():
    """Aggregates water meters to cadastre parcels and writes to temporary summary table"""
    try:
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')
        if not schema_name:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting water meter aggregation...")

        # Read base cadastre (lightweight, geometry only)
        logger.info("Loading cadastre base geometry...")
        gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel_uuid")

        # Read water meter data
        logger.info("Loading water meter data...")
        gdfWater = UtilDB.readFromDB(schema_name, "langata_water_meters")

        if gdfWater.empty:
            logger.warning("No water meters found - writing empty summary")
            empty_summary = gpd.GeoDataFrame(columns=['uuid','geom', 'water_count'])
            UtilDB.WriteToDB(empty_summary, "tmp_water_summary", "AggregateWater", if_exists="replace")
            return

        logger.info(f"Loaded {len(gdfWater)} water meters")

        # Perform spatial join
        logger.info("Performing spatial join...")
        joined_gdf = gpd.sjoin(
            gdfWater,
            gdfCadastre,
            how='inner',
            predicate='intersects'
        )
        logger.info(f"Found {len(joined_gdf)} water-cadastre intersections")

        # Aggregate by cadastre polygon (directly naming the column)
        logger.info("Aggregating water meters by parcel...")
        summary = (
            joined_gdf.groupby("uuid")
            .size()  
            .reset_index(name='water_count')
        )

        # Convert to GeoDataFrame by merging with cadastre geometries
        summary = gdfCadastre[['uuid','geom']].merge(
            summary,
            on='uuid',
            how="right"
        )

        # Fill NA values (for parcels with no water meters)
        summary['water_count'] = summary['water_count'].fillna(0).astype(int)

        logger.info(f"Processed {len(summary)} parcels with water meters")

        # Write to TEMPORARY table
        logger.info("Writing to temporary water summary table...")
        UtilDB.WriteToDB(
            summary,  # GeoDataFrame with geometry
            "water_summary",  # Temporary table name
            "AggregateWater",
            {"geometry":"geom"}
        )

        logger.info(f"Water aggregation complete. Total meters: {len(gdfWater)}")

    except Exception as e:
        logger.error(f"Water meter aggregation failed: {str(e)}", exc_info=True)
        raise
