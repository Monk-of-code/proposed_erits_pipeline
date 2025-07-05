import os
from pathlib import Path
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB
from . import InterestsOnCadastre

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')
schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')


def initialize():
    cadastre = UtilDB.readFromDB(
        f"{schema_name}", "langata_cp_power_water_bld_permit_address_uuid"
    )
    return cadastre


def main():
    cadastre = initialize()
    InterestsOnCadastre.PotentialInterestToCadastre(cadastre)
    InterestsOnCadastre.AggregateLandRates(cadastre)
    InterestsOnCadastre.AggregateTradeLicense(cadastre)
    InterestsOnCadastre.JoinFieldsToCadastre(cadastre)
    InterestsOnCadastre.CadastreDropDuplicates(cadastre)
    InterestsOnCadastre.DropNonRequiredFields(cadastre)
