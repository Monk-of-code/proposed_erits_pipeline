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

def AggregateDevelopmentControlApprovals():
    """Aggregates building permits to cadastre parcels and writes to temporary summary table"""
    try:
        schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')
        if not schema_name:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting development control approvals aggregation...")

        # Read base cadastre (lightweight, geometry only)
        logger.info("Loading cadastre base geometry...")
        gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel")

        # Read building permits data
        logger.info("Loading development control approvals...")
        gdfPermits = UtilDB.readFromDB(schema_name, "langata_building_permits")

        if gdfPermits.empty:
            logger.warning("No building permits found - writing empty summary")
            empty_summary = gpd.GeoDataFrame(columns=['index_right', 'permit_count'])
            UtilDB.WriteToDB(empty_summary, "tmp_permit_summary", "AggregateDevelopmentControlApprovals", if_exists="replace")
            return

        logger.info(f"Loaded {len(gdfPermits)} building permits")

        # Perform spatial join
        logger.info("Performing spatial join...")
        joined_gdf = gpd.sjoin(
            gdfPermits,
            gdfCadastre,
            how='inner',
            predicate='intersects'
        )
        logger.info(f"Found {len(joined_gdf)} permit-cadastre intersections")

        # Aggregate by cadastre polygon (directly naming the column)
        logger.info("Aggregating permits by parcel...")
        summary = (
            joined_gdf.groupby("index_right")
            .agg(permit_count=("index_right", "count"))  # Direct column naming
            .reset_index()
        )

        # Convert to GeoDataFrame by merging with cadastre geometries
        summary = gdfCadastre[['geom']].merge(
            summary,
            left_index=True,
            right_on="index_right",
            how="right"
        )

        # Fill NA values (for parcels with no permits)
        summary['permit_count'] = summary['permit_count'].fillna(0).astype(int)

        logger.info(f"Processed {len(summary)} parcels with permits")

        # Write to TEMPORARY table
        logger.info("Writing to temporary permit summary table...")
        UtilDB.WriteToDB(
            summary,  # GeoDataFrame with geometry
            "tmp_permit_summary",  # Temporary table name
            "AggregateDevelopmentControlApprovals",
             {"geometry":"geom"} # Overwrite existing
        )

        logger.info(f"Permit aggregation complete. Total permits: {len(gdfPermits)}")

    except Exception as e:
        logger.error(f"Development control approvals aggregation failed: {str(e)}", exc_info=True)
        raise