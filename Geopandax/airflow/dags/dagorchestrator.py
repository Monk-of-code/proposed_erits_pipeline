from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / '.env')

use_spark = os.getenv("USE_SPARK") == "1"

# Conditional imports
if use_spark:
    from Geopandax.Utilities import UtilDBSpark as UtilDB
    from Geopandax.DataPrep import BuildingsPrepSpark
else:
    from Geopandax.Utilities import UtilDB
    from Geopandax.DataPrep import (
        BuildingsPrep, CadastrePrep, PowerPrep,
        WaterPrep, DevelopmentPrep, Address
    )
    from Geopandax.AggregateOnCadastre import (
        AggregatePowerMeters, AggregateWaterMeters, AggregateBuildings,
        AggregateDevelopmentControl, AggregateAddress, ComputeFusionField
    )
    from Geopandax.CadastreUUID import CadastreUUIDMain
    from Geopandax.Interest import InterestMain, MoveToGold

# Shared variable 
g_selected = None

# --- Task Definitions ---
def initialize():
    global g_selected
    schema_name = os.getenv("PIPELINE_BRONZE_SCHEMA")
    gdf = UtilDB.readFromDB(schema_name, "au_subcounty_unit")
    g_selected = gdf[gdf["scouname"] == "Lang'ata"]
    UtilDB.WriteToDB(g_selected, "langata_au_subcounty_unit", "initialize", "")

def prep_cadastre(): CadastrePrep.Cadastre(g_selected)
def prep_water(): WaterPrep.Water(g_selected)
def prep_development(): DevelopmentPrep.DevelopmentControl(g_selected)
def prep_address(): Address.AddressLocations(g_selected)
def prep_buildings(): BuildingsPrep.Building(g_selected)
def prep_power(): PowerPrep.Power(g_selected)

def agg_power(): AggregatePowerMeters.Aggregate(g_selected)
def agg_water(): AggregateWaterMeters.Aggregate(g_selected)
def agg_buildings(): AggregateBuildings.Aggregate(g_selected)
def agg_development(): AggregateDevelopmentControl.Aggregate(g_selected)
def agg_address(): AggregateAddress.Aggregate(g_selected)

def add_fusion(): ComputeFusionField.Fusion(g_selected)
def generate_uuid(): CadastreUUIDMain.GenerateUUID(g_selected)
def join_interest(): InterestMain.InterestJoin(g_selected)

#  Wrapped  to avoid execution at parse time
def move_cadastre(): MoveToGold.MoveCadastreSync()
def move_development(): MoveToGold.MoveDevelopmentControlSync()

# Optional Spark mode
def spark_mode():
    print("Spark Mode Active")
    gdf = initialize()
    BuildingsPrepSpark.Building(gdf)

# --- DAG Definition ---
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="erits_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["erits", "geospatial"],
) as dag:

    if use_spark:
        run_spark = PythonOperator(
            task_id="spark_building_only",
            python_callable=spark_mode,
        )
    else:
        init = PythonOperator(task_id="initialize", python_callable=initialize)

        # Data prep
        t_cadastre = PythonOperator(task_id="prep_cadastre", python_callable=prep_cadastre)
        t_water = PythonOperator(task_id="prep_water", python_callable=prep_water)
        t_dev = PythonOperator(task_id="prep_development", python_callable=prep_development)
        t_address = PythonOperator(task_id="prep_address", python_callable=prep_address)
        t_buildings = PythonOperator(task_id="prep_buildings", python_callable=prep_buildings)
        t_power = PythonOperator(task_id="prep_power", python_callable=prep_power)

        # Aggregation
        a_power = PythonOperator(task_id="agg_power", python_callable=agg_power)
        a_water = PythonOperator(task_id="agg_water", python_callable=agg_water)
        a_buildings = PythonOperator(task_id="agg_buildings", python_callable=agg_buildings)
        a_address = PythonOperator(task_id="agg_address", python_callable=agg_address)
        a_dev = PythonOperator(task_id="agg_development", python_callable=agg_development)

        fusion = PythonOperator(task_id="add_fusion_field", python_callable=add_fusion)
        uuid = PythonOperator(task_id="generate_uuid", python_callable=generate_uuid)
        interest = PythonOperator(task_id="join_interest", python_callable=join_interest)

        move1 = PythonOperator(task_id="move_cadastre", python_callable=move_cadastre)
        move2 = PythonOperator(task_id="move_development", python_callable=move_development)

        # DAG Dependencies
        init >> [t_cadastre, t_water, t_dev, t_address, t_buildings, t_power]

        [t_cadastre, t_power] >> a_power
        [t_cadastre, t_water] >> a_water
        [t_cadastre, t_buildings] >> a_buildings
        [t_cadastre, t_address] >> a_address
        [t_cadastre, t_dev] >> a_dev

        [a_power, a_water, a_buildings, a_address, a_dev] >> fusion >> uuid >> interest >> [move1, move2]
