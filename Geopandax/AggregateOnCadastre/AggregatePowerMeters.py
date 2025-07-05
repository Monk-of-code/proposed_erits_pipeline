import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

def AggregatePower():
    # Read schema name from environment
    schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

    # Read cadastre data
    gdfCadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel")

    # Read power customers data
    gdfPower = UtilDB.readFromDB(schema_name, "langata_power_customers")

    # Perform spatial join
    joined_gdf = gpd.sjoin(
        gdfPower, gdfCadastre,
        how='inner', predicate='intersects'
    )

    # Aggregate by cadastre polygon
    summary = (
        joined_gdf.groupby("index_right")
        .agg(point_count=("index_right", "count"))
        .reset_index()
    )

    # Merge into cadastre layer
    gdfCadastre_with_summary = gdfCadastre.merge(
        summary,
        left_index=True,
        right_on="index_right",
        how="left"
    ).drop(columns=["index_right"])

    # Rename column
    gdfCadastre_with_summary.rename(columns={"point_count": "power_count"}, inplace=True)

    # Reset index
    gdfCadastre_with_summary.reset_index(drop=True, inplace=True)

    print(f'Power Meters count: {gdfPower.shape[0]}')

    # Save result to DB
    UtilDB.WriteToDB(
        gdfCadastre_with_summary,
        "langata_cp_power_water_bld_permit_address",
        "AggregatePower",
        ""
    )
