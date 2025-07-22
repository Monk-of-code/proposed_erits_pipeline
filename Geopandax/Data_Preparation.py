from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

use_spark = os.getenv("USE_SPARK") == "1"

# Conditional imports
if use_spark:
    from .Utilities import UtilDBSpark as UtilDB
    from .DataPrep import BuildingsPrepSpark
else:
    from Geopandax.Utilities import UtilDB
    from Geopandax.DataPrep import (
        BuildingsPrep,
        CadastrePrep,
        PowerPrep,
        WaterPrep,
        DevelopmentPrep,
        Address
    )
    from Geopandax.AggregateOnCadastre import (
        AggregatePowerMeters,
        AggregateWaterMeters,
        AggregateBuildings,
        AggregateDevelopmentControl,
        AggregateAddress,
        ComputeFusionField
    )
    from Geopandax.CadastreUUID import CadastreUUIDMain
    from Geopandax.Interest import InterestMain, MoveToGold


def initialize():
    """Sub County Preparation Section"""
    schema_name = os.getenv("PIPELINE_BRONZE_SCHEMA")
    gdf_subcounty = UtilDB.readFromDB(schema_name, "au_subcounty_unit")

    selected = gdf_subcounty[gdf_subcounty['scouname'] == "Lang'ata"]

    UtilDB.WriteToDB(selected, "langata_au_subcounty_unit", "initialize", "")
    return selected


def main():
    g_selected = initialize()

    # --- Data Preparation ---
    CadastrePrep.Cadastre(g_selected)
    WaterPrep.Water(g_selected)
    DevelopmentPrep.DevelopmentControl(g_selected)
    Address.AddressLocations(g_selected)
    BuildingsPrep.Building(g_selected)
    PowerPrep.Power(g_selected)

    # --- Aggregations ---
    AggregatePowerMeters.AggregatePower()
    AggregateWaterMeters.AggregateWater()
    AggregateBuildings.AggregateBuildings()
    AggregateAddress.AggregateAddressPoints()
    AggregateDevelopmentControl.AggregateDevelopmentControlApprovals()
    ComputeFusionField.AddMeasureFusionField()

    # --- UUID + Interest Join ---
    CadastreUUIDMain.main()
    InterestMain.main()
    MoveToGold.MoveCadastreSync()
    MoveToGold.MoveDevelopmentControlSync()


def main_spark():
    print("Spark Activated")
    g_selected = initialize()
    BuildingsPrepSpark.Building(g_selected)


# Entry point
if __name__ == "__main__":
    if use_spark:
        main_spark()
    else:
        main()
