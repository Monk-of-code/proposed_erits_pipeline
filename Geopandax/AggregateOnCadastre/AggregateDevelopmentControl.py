from dotenv import load_dotenv
import os
from pathlib import Path
import geopandas as gpd
import logging
from Geopandax.Utilities import UtilDB

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

def AggregateDevelopmentControlApprovals():
    # Schema to work in
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    # Load datasets
    gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permit_address")
    gdfDevelopmentControlApprovals = UtilDB.readFromDB(schema_name, "langata_building_permits")

    # Spatial join: find permits within cadastre polygons
    joined_gdf = gpd.sjoin(
        gdfDevelopmentControlApprovals, gdfCadastre,
        how='inner', predicate='intersects'
    )

    # Group and summarize by cadastre polygon
    summary = (
        joined_gdf.groupby("index_right")
        .agg(point_count=("index_right", "count"))
        .reset_index()
    )

    # Merge summary into cadastre
    gdfCadastre_with_summary = gdfCadastre.merge(
        summary,
        left_index=True,
        right_on="index_right",
        how="left"
    ).drop(columns=["index_right"]).rename(columns={"point_count": "permit_count"})

    # Reset index
    gdfCadastre_with_summary.reset_index(drop=True, inplace=True)

    print(f'Dev. Control Approvals count: {gdfDevelopmentControlApprovals.shape[0]}')
    print(f'Cadastre with Summary(DevelopmentControlApprovals): {gdfCadastre_with_summary.shape[0]}')

    # Save to DB
    UtilDB.WriteToDB(
        gdfCadastre_with_summary,
        "langata_cp_power_water_bld_permit_address",
        "AggregateDevelopmentControlApprovals",
        ""
    )
