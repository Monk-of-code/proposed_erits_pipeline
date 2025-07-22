from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path
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

def AggregateBuildings():
    """Aggregates buildings to cadastre parcels and writes to temporary summary table"""
    try:
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')
        if not schema_name:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting building aggregation...")

        # Read base cadastre (lightweight, geometry only)
        logger.info("Loading cadastre base geometry...")
        gdfCadastre = UtilDB.readFromDB(schema_name,"langata_cp_cadastral_parcel_uuid")

        # Read buildings data with timestamp filter
        logger.info("Loading buildings data...")
        gdfBuildings = UtilDB.readFromDBWithTimestamp(
            schema_name, "langata_buildings", "2025-02-04 00:00:00"
        )

        if gdfBuildings.empty:
            logger.warning("No buildings found - writing empty summary")
            empty_summary = gpd.GeoDataFrame(columns=['uuid','geom','building_count'])
            UtilDB.WriteToDB(empty_summary, "tmp_building_summary", "AggregateBuildings", if_exists="replace")
            return

        logger.info(f"Loaded {len(gdfBuildings)} buildings")

        # Perform spatial join
        logger.info("Performing spatial join...")
        joined_gdf = gpd.sjoin(
            gdfBuildings, 
            gdfCadastre, 
            how='inner', 
            predicate='intersects'
        )
        logger.info(f"Found {len(joined_gdf)} building-cadastre intersections")

        # Aggregate by cadastre polygon (directly naming the column)
        logger.info("Aggregating buildings by parcel...")
        summary = (
            joined_gdf.groupby("uuid")
            .size()  # Direct column naming
            .reset_index(name='building_count')
        )

        # Convert to GeoDataFrame by merging with cadastre geometries
        summary = gdfCadastre[['uuid','geom']].merge(
            summary,
            on='uuid',
            how="right"
        )

        # Fill NA values (for parcels with no buildings)
        summary['building_count'] = summary['building_count'].fillna(0).astype(int)

        logger.info(f"Processed {len(summary)} parcels with buildings")

        # Write to TEMPORARY table
        logger.info("Writing to temporary building summary table...")
        UtilDB.WriteToDB(
            summary,  # GeoDataFrame with geometry
            "building_summary",  # Temporary table name
            "AggregateBuildings",
             {"geometry":"geom"} 
        )

        logger.info(f"Building aggregation complete. Total buildings: {len(gdfBuildings)}")

    except Exception as e:
        logger.error(f"Building aggregation failed: {str(e)}", exc_info=True)
        raise