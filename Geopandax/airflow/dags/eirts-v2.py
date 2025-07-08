from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import timedelta
from pathlib import Path
import os
import geopandas as gpd
from dotenv import load_dotenv

# === Load .env ===
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / '.env')

# === Config ===
use_spark = os.getenv("USE_SPARK") == "1"
schema_name = os.getenv("PIPELINE_BRONZE_SCHEMA")
parquet_dir = os.getenv("PARQUET_FOLDER", "/opt/airflow/data")
parquet_file = os.path.join(parquet_dir, "g_selected.parquet")

# === Imports ===
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
        AggregateDevelopmentControl, AggregateAddress, ComputeFusionField,Joinsummaries
    )
    from Geopandax.CadastreUUID import CadastreUUIDMain
    from Geopandax.Interest import InterestMain, MoveToGold

# === Utility Functions ===
def load_selected():
    return gpd.read_parquet(parquet_file)

def ensure_parquet_dir():
    os.makedirs(parquet_dir, exist_ok=True)

# === Task Functions ===
def test_postgres_connection():
    try:
        hook = PostgresHook(postgres_conn_id="erits_postgres")
        conn = hook.get_conn()
        conn.close()
        print("Successfully connected to PostgreSQL")
        return True
    except Exception as e:
        raise AirflowException(f"PostgreSQL connection failed: {str(e)}")

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

def agg_power(): AggregatePowerMeters.AggregatePower()
def agg_water(): AggregateWaterMeters.AggregateWater()
def agg_buildings(): AggregateBuildings.AggregateBuildings()
def agg_address(): AggregateAddress.AggregateAddressPoints()
def agg_development(): AggregateDevelopmentControl.AggregateDevelopmentControlApprovals()
def join_all_summaries(): Joinsummaries.JoinAllSummaries()
def add_fusion(): ComputeFusionField.AddMeasureFusionField()
def generate_uuid(): CadastreUUIDMain.main()
def join_interest(): InterestMain.main()
def move_cadastre(): MoveToGold.MoveCadastreSync()
def move_development(): MoveToGold.MoveDevelopmentControlSync()

# === DAG Setup ===
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
    dag_id="erits",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["erits", "geospatial"],
    doc_md="### ERITS Geospatial Pipeline\nProcesses subcounty features, utilities, cadastre, buildings."
) as dag:

    test_connection = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_connection
    )

    init = PythonOperator(
        task_id="initialize",
        python_callable=initialize
    )

    t_cadastre = PythonOperator(task_id="prep_cadastre", python_callable=prep_cadastre)
    t_water = PythonOperator(task_id="prep_water", python_callable=prep_water)
    t_dev = PythonOperator(task_id="prep_development", python_callable=prep_development)
    t_address = PythonOperator(task_id="prep_address", python_callable=prep_address)
    t_buildings = PythonOperator(task_id="prep_buildings", python_callable=prep_buildings)
    t_power = PythonOperator(task_id="prep_power", python_callable=prep_power)

    a_power = PythonOperator(task_id="agg_power", python_callable=agg_power)
    a_water = PythonOperator(task_id="agg_water", python_callable=agg_water)
    a_buildings = PythonOperator(task_id="agg_buildings", python_callable=agg_buildings)
    a_address = PythonOperator(task_id="agg_address", python_callable=agg_address)
    a_dev = PythonOperator(task_id="agg_development", python_callable=agg_development)

    join_summaries = PythonOperator(task_id="join_all_summaries", python_callable=join_all_summaries)

    fusion = PythonOperator(task_id="add_fusion_field", python_callable=add_fusion)
    uuid = PythonOperator(task_id="generate_uuid", python_callable=generate_uuid)
    interest = PythonOperator(task_id="join_interest", python_callable=join_interest)

    move1 = PythonOperator(task_id="move_cadastre", python_callable=move_cadastre)
    move2 = PythonOperator(task_id="move_development", python_callable=move_development)

    # === DAG Dependencies ===
    test_connection >> init

    init >> [t_cadastre, t_water, t_dev, t_address, t_buildings, t_power]

    [t_cadastre, t_power] >> a_power
    [t_cadastre, t_water] >> a_water
    [t_cadastre, t_buildings] >> a_buildings
    [t_cadastre, t_address] >> a_address
    [t_cadastre, t_dev] >> a_dev

    [a_power, a_water, a_buildings, a_address, a_dev] >> join_summaries>>fusion >> uuid >> interest >> [move1, move2]

    # Attach failure callback to all tasks
    for task in dag.tasks:
        task.on_failure_callback = failure_callback
