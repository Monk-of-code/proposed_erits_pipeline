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
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    # Read cadastre data
    gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permit_address")

    # Read water meter points
    gdfWaterMeters = UtilDB.readFromDB(schema_name, "langata_water_meters")

    # Spatial join
    joined_gdf = gpd.sjoin(
        gdfWaterMeters, gdfCadastre,
        how='inner', predicate='intersects'
    )

    # Summarize point count per polygon
    summary = (
        joined_gdf.groupby("index_right")
        .agg(point_count=("index_right", "count"))
        .reset_index()
    )

    # Merge counts into cadastre polygons
    gdfCadastre_with_summary = gdfCadastre.merge(
        summary,
        left_index=True,
        right_on="index_right",
        how="left"
    ).drop(columns=["index_right"])

    gdfCadastre_with_summary.rename(columns={"point_count": "water_count"}, inplace=True)
    gdfCadastre_with_summary.reset_index(drop=True, inplace=True)

    print(f'Water Meters count: {gdfWaterMeters.shape[0]}')

    # Write result to DB
    UtilDB.WriteToDB(
        gdfCadastre_with_summary,
        "langata_cp_power_water_bld_permit_address",
        "AggregateWater",
        ""
    )
