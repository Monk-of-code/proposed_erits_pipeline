from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from datetime import timedelta
from pathlib import Path
import os
import geopandas as gpd
from dotenv import load_dotenv

# Load .env
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / '.env')

# Config
schema_name = os.getenv("PIPELINE_BRONZE_SCHEMA")
parquet_dir = os.getenv("PARQUET_FOLDER", "/opt/airflow/data")
parquet_file = os.path.join(parquet_dir, "g_selected.parquet")

from Geopandax.Utilities import UtilDB
from Geopandax.DataPrep import BuildingsPrep, CadastrePrep, PowerPrep, WaterPrep, DevelopmentPrep, Address
from Geopandax.AggregateOnCadastre import AggregatePowerMeters, AggregateWaterMeters, AggregateBuildings, AggregateDevelopmentControl, AggregateAddress, ComputeFusionField, Joinsummaries
from Geopandax.CadastreUUID import CadastreUUIDMain
from Geopandax.CadastreUUID.AddAdressDataToCadastre import AddAddressPropertyNameToCadastre
from Geopandax.CadastreUUID.AddUUID import KPLC,NWSC,AddressPointsUsingJoinAttributeByNearestParcel,AddressPointsUsingJoinAttributeByNearestParcel2,BuildingFootPrints,DevelopmentControl
from Geopandax.Interest.InterestsOnCadastre import DropNonRequiredFields, CadastreDropDuplicates
from Geopandax.Interest.MoveToGold import MoveCadastreSync, MovePowerSync, MoveWaterSync, MoveDevelopmentControlSync

# Utility functions
def load_selected():
    return gpd.read_parquet(parquet_file)

def ensure_parquet_dir():
    os.makedirs(parquet_dir, exist_ok=True)

def address_uuid_with_fallback():
    try:
        AddressPointsUsingJoinAttributeByNearestParcel2(...)
    except Exception:
        AddressPointsUsingJoinAttributeByNearestParcel(...)

#Task wrapper functions
def initialize():
    try:
        ensure_parquet_dir()
        gdf = UtilDB.readFromDB(schema_name, "au_subcounty_unit")
        g_selected = gdf[gdf["scouname"] == "Lang'ata"]
        g_selected.to_parquet(parquet_file)
        UtilDB.WriteToDB(g_selected, "langata_au_subcounty_unit", "initialize", "")
    except Exception as e:
        raise AirflowException(f"Initialization failed: {str(e)}")

def prep_cadastre():
    try:
        g_selected = load_selected()
        CadastrePrep.Cadastre(g_selected)
    except Exception as e:
        raise AirflowException(f"Cadastre prep failed: {str(e)}")

def prep_water():
    try:
        g_selected = load_selected()
        WaterPrep.Water(g_selected)
    except Exception as e:
        raise AirflowException(f"Water prep failed: {str(e)}")

def prep_development():
    try:
        g_selected = load_selected()
        DevelopmentPrep.DevelopmentControl(g_selected)
    except Exception as e:
        raise AirflowException(f"Development prep failed: {str(e)}")

def prep_address():
    try:
        g_selected = load_selected()
        Address.AddressLocations(g_selected)
    except Exception as e:
        raise AirflowException(f"Address prep failed: {str(e)}")

def prep_buildings():
    try:
        g_selected = load_selected()
        BuildingsPrep.Building(g_selected)
    except Exception as e:
        raise AirflowException(f"Buildings prep failed: {str(e)}")

def prep_power():
    try:
        g_selected = load_selected()
        PowerPrep.Power(g_selected)
    except Exception as e:
        raise AirflowException(f"Power prep failed: {str(e)}")
    
    
# DAG Setup
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def failure_callback(context):
    task_instance = context.get("task_instance")
    print(f" Task {task_instance.task_id} failed!")

with DAG(
    dag_id="erits_proposed",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["erits", "geospatial"],
    doc_md="""### ERITS Geospatial Pipeline\nAutomates extraction, preparation, linking, scoring, and exporting of enriched cadastre features for Lang'ata Sub-County"""
) as dag:

    init = PythonOperator(task_id="initialize", python_callable=ensure_parquet_dir)

    # ===== Task Group 1: Data Preparation =====
    with TaskGroup("data_preparation") as prep_group:
        
        prep_cadastre = PythonOperator(task_id="prep_cadastre", python_callable=CadastrePrep.Cadastre)
        prep_power = PythonOperator(task_id="prep_power", python_callable=PowerPrep.Power)
        prep_water = PythonOperator(task_id="prep_water", python_callable=WaterPrep.Water)
        prep_buildings = PythonOperator(task_id="prep_buildings", python_callable=BuildingsPrep.Building)
        prep_permits = PythonOperator(task_id="prep_permits", python_callable=DevelopmentPrep.DevelopmentControl)
        prep_address = PythonOperator(task_id="prep_address", python_callable=Address.AddressLocations)

    # ===== Task Group 2: UUID Processing =====
    with TaskGroup("uuid_processing") as uuid_group:
        generate_uuid = PythonOperator(task_id="generate_uuid", python_callable=CadastreUUIDMain.main)

        with TaskGroup("feature_linking") as link_group:
            link_power = PythonOperator(task_id="link_power", python_callable=KPLC)
            link_water = PythonOperator(task_id="link_water", python_callable=NWSC)
            link_buildings = PythonOperator(task_id="link_buildings", python_callable=BuildingFootPrints)
            link_permits = PythonOperator(task_id="link_permits", python_callable=DevelopmentControl)
            link_address = PythonOperator(task_id="link_address", python_callable=address_uuid_with_fallback)

    # ===== Task Group 3: Aggregation =====
    with TaskGroup("aggregation") as agg_group:
        with TaskGroup("feature_aggregation") as feat_agg_group:
            agg_power = PythonOperator(task_id="agg_power", python_callable=AggregatePowerMeters.AggregatePower)
            agg_water = PythonOperator(task_id="agg_water", python_callable=AggregateWaterMeters.AggregateWater)
            agg_buildings = PythonOperator(task_id="agg_buildings", python_callable=AggregateBuildings.AggregateBuildings)
            agg_address = PythonOperator(task_id="agg_address", python_callable=AggregateAddress.AggregateAddressPoints)
            agg_permits = PythonOperator(task_id="agg_permits", python_callable=AggregateDevelopmentControl.AggregateDevelopmentControlApprovals)

        join_summaries = PythonOperator(task_id="join_summaries", python_callable=Joinsummaries.JoinAllSummaries)

    # ===== Task Group 4: Final Processing =====
    with TaskGroup("final_processing") as final_group:
        drop_duplicates = PythonOperator(task_id="drop_duplicates", python_callable=CadastreDropDuplicates)
        add_fusion = PythonOperator(task_id="add_fusion", python_callable=ComputeFusionField.AddMeasureFusionField)
        add_property_name = PythonOperator(task_id="add_property_name", python_callable=AddAddressPropertyNameToCadastre)
        drop_fields = PythonOperator(task_id="drop_fields", python_callable=DropNonRequiredFields)

    # ===== Task Group 5: Move to Gold =====
    with TaskGroup("move_to_gold") as move_group:
        move_cadastre = PythonOperator(task_id="move_cadastre", python_callable=MoveCadastreSync)
        move_power = PythonOperator(task_id="move_power", python_callable=MovePowerSync)
        move_water = PythonOperator(task_id="move_water", python_callable=MoveWaterSync)
        move_permits = PythonOperator(task_id="move_permits", python_callable=MoveDevelopmentControlSync)

    # ===== Dependencies =====
    init >> prep_group
    prep_group >> generate_uuid
    generate_uuid >> link_group

    link_power >> agg_power
    prep_cadastre >> agg_power

    link_water >> agg_water
    prep_cadastre >> agg_water

    link_buildings >> agg_buildings
    prep_cadastre >> agg_buildings

    link_permits >> agg_permits
    prep_cadastre >> agg_permits

    link_address >> agg_address
    prep_cadastre >> agg_address

    feat_agg_group >> join_summaries
    join_summaries >> drop_duplicates >> add_fusion >> add_property_name >> drop_fields >> move_group

    for task in dag.tasks:
        task.on_failure_callback = failure_callback
