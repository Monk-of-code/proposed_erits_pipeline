from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from datetime import timedelta
from pathlib import Path
import os
import geopandas as gpd
from dotenv import load_dotenv
import sys
from pathlib import Path



# Load environment variables
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / '.env')
load_dotenv(BASE_DIR / '.env.secrets')

# Config
use_spark = os.getenv("USE_SPARK") == "1"
schema_name = os.getenv("PIPELINE_BRONZE_SCHEMA")
parquet_dir = os.getenv("PARQUET_FOLDER", "/opt/airflow/data")
parquet_file = os.path.join(parquet_dir, "g_selected.parquet")

# Imports
from Geopandax.Utilities import UtilDB
from Geopandax.DataPrep import BuildingsPrep, CadastrePrep, PowerPrep, WaterPrep, DevelopmentPrep, Address
from Geopandax.AggregateOnCadastre import (
    AggregatePowerMeters, AggregateWaterMeters, AggregateBuildings,
    AggregateDevelopmentControl, AggregateAddress, ComputeFusionField, Joinsummaries
)
from Geopandax.CadastreUUID.AddUUID import (
    AddUUIDToCadastre, KPLC, NWSC, BuildingFootPrints,
    DevelopmentControl, AddressPoints,
    AddressPointsUsingJoinAttributeByNearestParcel2
)
from Geopandax.CadastreUUID import AddAdressDataToCadastre
from Geopandax.Interest.InterestsOnCadastre import (
    PotentialInterestToCadastre, JoinFieldsToCadastre, AggregateLandRates,
    AggregateTradeLicense, CadastreDropDuplicates, DropNonRequiredFields
)
from Geopandax.Interest.MoveToGold import (
    MoveCadastreSync, MoveDevelopmentControlSync, MovePowerSync, MoveWaterSync
)

# Parquet utility functions
def load_selected():
    return gpd.read_parquet(parquet_file)

def ensure_parquet_dir():
    os.makedirs(parquet_dir, exist_ok=True)

# Core Task Functions
def test_postgres_connection():
    try:
        hook = PostgresHook(postgres_conn_id="erits_postgres")
        conn = hook.get_conn()
        conn.close()
        print("Successfully connected to PostgreSQL")
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

def loadCadastreToParquet():
    try:
        ensure_parquet_dir()
        schema_name = os.getenv("PIPELINE_SILVER_SCHEMA")
        cadastre = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel")
        cadastre.to_parquet(f"{parquet_dir}/cadastre.parquet")
    except Exception as e:
        raise AirflowException(f"Failed to write cadastre parquet: {str(e)}")

def loadCadastreuuidToParquet():
    try:
        ensure_parquet_dir()
        schema_name = os.getenv("PIPELINE_SILVER_SCHEMA")
        cadastreUUID = UtilDB.readFromDB(schema_name, "langata_cp_cadastral_parcel_uuid")
        cadastreUUID.to_parquet(f"{parquet_dir}/cadastreUUID.parquet")
    except Exception as e:
        raise AirflowException(f"Failed to write cadastreUUID parquet: {str(e)}")
    
def loadlangataaggjoined():
    try:
        ensure_parquet_dir()
        schema_name =os.getenv("PIPELINE_SILVER_SCHEMA")
        cadastre=UtilDB.readFromDB(schema_name,"langata_cp_power_water_bld_permits_address_agg_uuid")
        return cadastre
    except Exception as e:
        raise AirflowException(f"Failed to read langata_cp_power_water_bld_permits_address_agg_uuid: {str(e)}")    

def load_cadastre():
    return gpd.read_parquet(f"{parquet_dir}/cadastre.parquet")

def load_cadastre_uuid():
    return gpd.read_parquet(f"{parquet_dir}/cadastreUUID.parquet")


def failure_callback(context):
    task_instance = context.get("task_instance")
    print(f"Task {task_instance.task_id} failed!")

# DAG Definition
with DAG(
    dag_id="erits_proposed",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["erits", "geospatial"],
    doc_md="""### ERITS Geospatial Pipeline\nProcesses subcounty features, utilities, cadastre, buildings."""
) as dag:

    test_connection = PythonOperator(task_id="test_postgres_connection", python_callable=test_postgres_connection)
    init = PythonOperator(task_id="initialize", python_callable=initialize)

    with TaskGroup("parquet_handling") as parquet_group:
        pass
    
        write_cadastre = PythonOperator(task_id="write_cadastre_parquet", python_callable=loadCadastreToParquet,dag=dag)
        write_uuid = PythonOperator(task_id="write_cadastreUUID_parquet", python_callable=loadCadastreuuidToParquet,dag=dag)
        #  Manually assign tasks to the TaskGroup for UI grouping
        write_cadastre.task_group = parquet_group
        write_uuid.task_group = parquet_group
        

    with TaskGroup("data_preparation") as prep_group:
        PythonOperator(task_id="cadastre", python_callable=lambda: CadastrePrep.Cadastre(load_selected()))
        PythonOperator(task_id="water", python_callable=lambda: WaterPrep.Water(load_selected()))
        PythonOperator(task_id="development", python_callable=lambda: DevelopmentPrep.DevelopmentControl(load_selected()))
        PythonOperator(task_id="address", python_callable=lambda: Address.AddressLocations(load_selected()))
        PythonOperator(task_id="buildings", python_callable=lambda: BuildingsPrep.Building(load_selected()))
        PythonOperator(task_id="power", python_callable=lambda: PowerPrep.Power(load_selected()))

    with TaskGroup("generate_uuid") as generate_uuid_group:
        PythonOperator(task_id="add_uuid_to_cadastre", python_callable=lambda:AddUUIDToCadastre(load_cadastre()))

    with TaskGroup("link_uuid") as link_uuid_group:
        PythonOperator(task_id="link_kplc", python_callable=lambda:KPLC(load_cadastre_uuid()))
        PythonOperator(task_id="link_nwsc", python_callable=lambda:NWSC(load_cadastre_uuid()))
        PythonOperator(task_id="link_buildings", python_callable=lambda:BuildingFootPrints(load_cadastre_uuid()))
        PythonOperator(task_id="link_development", python_callable=lambda:DevelopmentControl(load_cadastre_uuid()))

    with TaskGroup("address_preparation") as address_uuid_group:
        link_address_nearest = PythonOperator(task_id="link_address_intersect", python_callable=lambda:AddressPoints(load_cadastre_uuid()))
        link_address_intersect = PythonOperator(task_id="link_address_nearest", python_callable=lambda:AddressPointsUsingJoinAttributeByNearestParcel2(load_cadastre()))

    with TaskGroup("aggregates") as agg_group:
        PythonOperator(task_id="power", python_callable=AggregatePowerMeters.AggregatePower)
        PythonOperator(task_id="water", python_callable=AggregateWaterMeters.AggregateWater)
        PythonOperator(task_id="buildings", python_callable=AggregateBuildings.AggregateBuildings)
        PythonOperator(task_id="address", python_callable=AggregateAddress.AggregateAddressPoints)
        PythonOperator(task_id="development", python_callable=AggregateDevelopmentControl.AggregateDevelopmentControlApprovals)

    with TaskGroup("join_aggregates") as join_group:
        PythonOperator(task_id="join_summaries", python_callable=Joinsummaries.JoinAllSummaries) >> \
        PythonOperator(task_id="add_fusion", python_callable=ComputeFusionField.AddMeasureFusionField) >> \
        PythonOperator(task_id="add_propertyname", python_callable=AddAdressDataToCadastre.AddAddressPropertyNameToCadastre)

    with TaskGroup("interest") as interest_group:
        join_field_task = PythonOperator(task_id="join_fields_landrate", python_callable=lambda:JoinFieldsToCadastre(loadlangataaggjoined()))
        potential_interest_to_cadastre_task=PythonOperator(task_id="potential_interest_to_cadastre", python_callable=lambda:PotentialInterestToCadastre(loadlangataaggjoined()))
        aggregate_land_rates_task=PythonOperator(task_id="aggregate_landrates", python_callable=lambda:AggregateLandRates(loadlangataaggjoined()))
        aggregate_trade_licence_task=PythonOperator(task_id="aggregate_trade_license", python_callable=lambda:AggregateTradeLicense(loadlangataaggjoined()))
        
        potential_interest_to_cadastre_task >> aggregate_land_rates_task >> aggregate_trade_licence_task >> join_field_task 
    with TaskGroup("postprocessing") as post_group:
        PythonOperator(task_id="drop_duplicates", python_callable=lambda:CadastreDropDuplicates(loadlangataaggjoined())) >> \
        PythonOperator(task_id="clear_unwanted_rows", python_callable=lambda:DropNonRequiredFields(loadlangataaggjoined()))

    with TaskGroup("move_to_gold") as move_group:
        PythonOperator(task_id="cadastre", python_callable=MoveCadastreSync)
        PythonOperator(task_id="development", python_callable=MoveDevelopmentControlSync)
        PythonOperator(task_id="power", python_callable=MovePowerSync)
        PythonOperator(task_id="water", python_callable=MoveWaterSync)

    # DAG flow
        test_connection >> init >> prep_group

# parquet writing after cadastre prep
        prep_group >> write_cadastre

# parquet writing after UUID generation
        generate_uuid_group >> write_uuid
# Continue flow
        # Only define the DAG structure once and cleanly:
        prep_group >> write_cadastre >> generate_uuid_group >>write_uuid>>[link_uuid_group, address_uuid_group] >> agg_group >> join_group
        join_group >> interest_group >> post_group >> move_group


    

    for task in dag.tasks:
        task.on_failure_callback = failure_callback


