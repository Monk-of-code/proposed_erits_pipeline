import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import geopandas as gpd
from Geopandax.Utilities import UtilDB

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load .env
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)


def AggregateAddressPoints():
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    # Read cadastre + address data
    gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permit_address")
    gdfAddress = UtilDB.readFromDB(schema_name, "langata_address_locations")

    # Spatial join
    joined_gdf = gpd.sjoin(gdfAddress, gdfCadastre, how='inner', predicate='intersects')

    # Group and summarize
    summary = (
        joined_gdf.groupby("index_right")
        .agg(point_count=("index_right", "count"))
        .reset_index()
    )

    # Merge results
    gdfCadastre_with_summary = gdfCadastre.merge(
        summary,
        left_index=True,
        right_on="index_right",
        how="left"
    ).drop(columns=["index_right"]).rename(columns={"point_count": "address_count"})

    gdfCadastre_with_summary.reset_index(drop=True, inplace=True)

    print(f'Address count: {gdfAddress.shape[0]}')
    print(f'Cadastre with Summary(AddressPoints): {gdfCadastre_with_summary.shape[0]}')

    # Write results to DB
    UtilDB.WriteToDB(
        gdfCadastre_with_summary,
        "langata_cp_power_water_bld_permit_address",
        "AggregateAddressPoints",
        ""
    )
