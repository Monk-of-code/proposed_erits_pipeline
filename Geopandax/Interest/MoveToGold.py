import os
from pathlib import Path
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load .env variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

# Schema name
SCHEMA_SILVER = os.getenv('PIPELINE_SILVER_SCHEMA')
SCHEMA_GOLD=os.getenv('PIPELINE_GOLD_SCHEMA')

# --------------------- CADASTRE MOVERS --------------------- #



def MoveCadastreSync():
    UtilDB.migrate_table(
        source_schema=SCHEMA_SILVER,
        source_table='langata_cp_power_water_bld_permits_address_agg_uuid',
        target_schema=SCHEMA_GOLD,
        target_table='langata_cp_kplc_nwsc_bldg_permits_address_gold'
    )


def MoveWaterSync():
    UtilDB.migrate_table(
        source_schema=SCHEMA_SILVER,
        source_table="langata_water_meters_with_nlims_link",
        target_schema=SCHEMA_GOLD,
        target_table="langata_water_meters_with_nlims_link_gold"
        )



# --------------------- POWER, WATER, DEVELOPMENT CONTROL --------------------- #

def MovePowerSync():
    UtilDB.migrate_table(
        source_schema=SCHEMA_SILVER,
        source_table="langata_power_customers_with_nlims_link",
        target_schema=SCHEMA_GOLD,
        target_table="langata_power_customers_with_nlims_link_gold"
    )

def MoveDevelopmentControlSync():
    UtilDB.migrate_table(
        source_schema=SCHEMA_SILVER,
        source_table="langata_building_permits_with_nlims_link",
        target_schema=SCHEMA_GOLD,
        target_table="langata_building_permits_with_nlims_link_gold",
        )
