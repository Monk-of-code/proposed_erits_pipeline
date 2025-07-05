import os
from pathlib import Path
import logging
import geopandas as gpd
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

SCHEMA_SILVER = os.getenv('PIPELINE_SILVER_SCHEMA')
SCHEMA_BRONZE = os.getenv('PIPELINE_BRONZE_SCHEMA')


def PotentialInterestToCadastre(gdf_cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid")
    print(f'Cadastre count before PotentialInterestToCadastre: {gdf_cadastre.shape[0]}')

    gdf_attributes = UtilDB.readNonSpatialFromDB(SCHEMA_BRONZE, "bd_land_rate_data")
    gdf_attributes['uuid'] = gdf_attributes['uuid_derived']

    gdf_joined = gdf_cadastre.merge(gdf_attributes, on="uuid", how="left")
    gdf_joined_deep = gdf_joined.copy(deep=True)

    filtered_gdf = gdf_joined_deep[
        gdf_joined_deep[['KRA PIN', 'Customer ID', 'Customer No']].notnull().all(axis=1)
    ].copy(deep=True)

    UtilDB.WriteToDB(filtered_gdf, "langata_potential_interests_to_land", "PotentialInterestToCadastre_Spatial", "")

    filtered_gdf = filtered_gdf.drop(columns='geom')
    columns_to_keep = [
        "lr_number", "parcel_number", "uuid", "LRNumber", "LR Amount", "LR Invoice No",
        "Customer No", "Customer No1", "KRA PIN", "Customer ID", "Phone Number",
        "uuid_derived", "Property Value"
    ]
    filtered_gdf = filtered_gdf[columns_to_keep]

    filtered_gdf.rename(columns={
        "uuid_derived": "BL_uuid_derived",
        "Property Value": "BL_PropertyValue",
        "Phone Number": "BL_PhoneNumber",
        "Customer ID": "BL_CustomerID",
        "KRA PIN": "BL_KRAPIN",
        "Customer No1": "BL_CustomerNo1",
        "Customer No": "BL_CustomerNo",
        "LR Invoice No": "BL_LRInvoiceNo",
        "LR Amount": "BL_LRAmount",
        "LRNumber": "BL_LRNumber"
    }, inplace=True)

    UtilDB.WriteNonSpatialToDB(filtered_gdf, "potential_interests_to_cadastre", "PotentialInterestToCadastre-NonSpatial")


def JoinFieldsToCadastre(gdf_cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid")
    print(f'Cadastre count before JoinFieldsToCadastre: {gdf_cadastre.shape[0]}')

    gdf_attributes = UtilDB.readNonSpatialFromDB(SCHEMA_BRONZE, "bd_land_rate_data")
    gdf_attributes['uuid'] = gdf_attributes['uuid_derived']

    gdf_joined = gdf_cadastre.merge(gdf_attributes, on="uuid", how="left")
    gdf_joined_deep = gdf_joined.copy(deep=True)

    gdf_joined_deep.rename(columns={
        "uuid_derived": "BL_uuid_derived",
        "Property Value": "BL_PropertyValue",
        "Phone Number": "BL_PhoneNumber",
        "Customer ID": "BL_CustomerID",
        "KRA PIN": "BL_KRAPIN",
        "Customer No1": "BL_CustomerNo1",
        "Customer No": "BL_CustomerNo",
        "LR Invoice No": "BL_LR InvoiceNo",
        "LR Amount": "BL_LR Amount",
        "LRNumber": "BL_LRNumber"
    }, inplace=True)

    gdf_joined_deep['owner'] = ""
    UtilDB.WriteToDB(gdf_joined_deep, "langata_cp_power_water_bld_permit_address_uuid", "JoinFieldsToCadastre", "")
    print(f'Cadastre count AFTER JoinFieldsToCadastre: {gdf_joined_deep.shape[0]}')


def AggregateLandRates(cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid")
    print(f'Cadastre count before AggregateLandRates: {gdf_cadastre.shape[0]}')

    gdf_attributes = UtilDB.readNonSpatialFromDB(SCHEMA_BRONZE, "bd_land_rate_data")
    gdf_attributes = gdf_attributes.rename(columns={'uuid_derived': 'uuid'})

    feature_counts = gdf_attributes['uuid'].value_counts().reset_index()
    feature_counts.columns = ['uuid', 'rates_count']

    gdf_cadastre = gdf_cadastre.merge(feature_counts, on='uuid', how='left')
    gdf_cadastre['rates_count'] = gdf_cadastre['rates_count'].fillna(0).astype(int)

    UtilDB.WriteToDB(gdf_cadastre, "langata_cp_power_water_bld_permit_address_uuid", "AggregateLandRates", "")


def AggregateTradeLicense(cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid")

    gdf_attributes = UtilDB.readFromDB(SCHEMA_BRONZE, "trade_license")

    feature_counts = gdf_attributes['uuid'].value_counts().reset_index()
    feature_counts.columns = ['uuid', 'trade_count']

    gdf_cadastre = gdf_cadastre.merge(feature_counts, on='uuid', how='left')
    gdf_cadastre['trade_count'] = gdf_cadastre['trade_count'].fillna(0).astype(int)

    UtilDB.WriteToDB(gdf_cadastre, "langata_cp_power_water_bld_permit_address_uuid", "AggregateTradeLicense", "")


def CadastreDropDuplicates(cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid")

    gdf_cadastre = gdf_cadastre.drop_duplicates(subset='uuid').copy(deep=True)

    UtilDB.WriteToDB(gdf_cadastre, "langata_cp_power_water_bld_permit_address_uuid_unique", "CadastreDropDuplicates", "")

    print(f'uuid uniqueness: {gdf_cadastre["uuid"].is_unique}')


def DropNonRequiredFields(cadastre):
    gdf_cadastre = UtilDB.readFromDB(SCHEMA_SILVER, "langata_cp_power_water_bld_permit_address_uuid_unique")

    columns_to_keep = [
        'fid', 'geom', 'fr_number', 'rim_name', 'lr_number', 'parcel_number', 'sheet_number', 'stated_area',
        'area_type', 'comments', 'null_comment', 'reg_block', 'block_name', 'bound_type', 'feature_type',
        'deed_plan_', 'county', 'sub_county', 'sub_location', 'dmp_number', 'original_number', 'transaction',
        'old_parcel', 'owner', 'current_status', 'shape_leng', 'shape_area', 'dp_number', 'power_count',
        'water_count', 'building_count', 'permit_count', 'address_count', 'uuid', 'BL_LRNumber', 'BL_LR Amount',
        'BL_LR InvoiceNo', 'BL_CustomerNo', 'BL_CustomerNo1', 'BL_KRAPIN', 'BL_CustomerID', 'BL_PhoneNumber',
        'BL_PropertyValue', 'BL_uuid_derived', 'fusion', 'propertyName', 'rates_count', 'trade_count'
    ]
     # Keep only columns that exist
    existing_columns = [col for col in columns_to_keep if col in gdf_cadastre.columns]
    missing_columns = [col for col in columns_to_keep if col not in gdf_cadastre.columns]

    if missing_columns:
        logger.warning(f"The following expected columns are missing and will be skipped: {missing_columns}")

    gdf_cadastre = gdf_cadastre[existing_columns]

    UtilDB.WriteToDB(gdf_cadastre, "langata_cp_power_water_bld_permit_address_uuid_unique", "DropNonRequiredFields", "")
