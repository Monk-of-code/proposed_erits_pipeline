import os
from pathlib import Path
import geopandas as gpd
import asyncio
from dotenv import load_dotenv

from Geopandax.Utilities import UtilDB
from . import AddUUID
from . import AddAdressDataToCadastre

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')


def initialize():
    # Get cadastre dataset from DB
    cadastre = UtilDB.readFromDB(schema_name, "langata_cp_power_water_bld_permit_address")
    return cadastre


def main():
    cadastre = initialize()
    cadastre_uuid = AddUUID.AddUUIDToCadastre(cadastre)
    
    AddUUID.KPLC(cadastre_uuid)
    AddUUID.NWSC(cadastre_uuid)
    AddUUID.BuildingFootPrints(cadastre_uuid)
    AddUUID.DevelopmentControl(cadastre_uuid)
    AddUUID.AddressPoints(cadastre_uuid)
    AddUUID.AddressPointsUsingJoinAttributeByNearestParcel2(cadastre_uuid)
    AddAdressDataToCadastre.AddAddressPropertyNameToCadastre()
