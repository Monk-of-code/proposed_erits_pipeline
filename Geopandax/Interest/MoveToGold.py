import os
from pathlib import Path
from dotenv import load_dotenv
from Geopandax.Utilities import UtilDB

# Load .env variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

# Schema name
SCHEMA_SILVER = os.getenv('PIPELINE_SILVER_SCHEMA')

# --------------------- CADASTRE MOVERS --------------------- #

async def MoveCadastreAsync():
    field_mapping = _get_cadastre_field_mapping()
    await UtilDB.copy_from_table_a_to_table_b(
        "langata_cp_power_water_bld_permit_address_uuid_unique",
        "langata_cp_kplc_nwsc_bldg_permits_address",
        SCHEMA_SILVER, field_mapping)


def MoveCadastreSync():
    field_mapping = _get_cadastre_field_mapping()
    UtilDB.CopyFromTableAtoTableB(
        "langata_cp_power_water_bld_permit_address_uuid_unique",
        "langata_cp_kplc_nwsc_bldg_permits_address",
        SCHEMA_SILVER, field_mapping)


# --------------------- POWER, WATER, DEVELOPMENT CONTROL --------------------- #

def MovePowerSync():
    field_mapping = _get_power_field_mapping()
    UtilDB.CopyFromTableAtoTableB(
        "langata_power_customers_with_nlims_link",
        "langata_power_customers_v4_with_nlims_link",
        SCHEMA_SILVER, field_mapping)


def MoveWaterSync():
    field_mapping = _get_water_field_mapping()
    UtilDB.CopyFromTableAtoTableB(
        "langata_water_meters_with_nlims_link",
        "langata_water_meters_v4_with_nlims_link",
        SCHEMA_SILVER, field_mapping)


def MoveDevelopmentControlSync():
    field_mapping = _get_dev_control_field_mapping()
    UtilDB.CopyFromTableAtoTableB(
        "langata_building_permits_with_nlims_link",
        "building_permits_v4_with_nlims_link",
        SCHEMA_SILVER, field_mapping)


async def MoveDevelopmentControlAsync():
    field_mapping = _get_dev_control_field_mapping()
    await UtilDB.copy_from_table_a_to_table_b(
        "langata_building_permits_with_nlims_link",
        "building_permits_v4_with_nlims_link",
        SCHEMA_SILVER, field_mapping)


# --------------------- FIELD MAPPINGS --------------------- #

def _get_cadastre_field_mapping():
    return {
        'geom': 'geom', 'fr_number': 'fr_number', 'rim_name': 'rim_name',
        'lr_number': 'lr_number', 'parcel_number': 'parcel_number', 'sheet_number': 'sheet_number',
        'stated_area': 'stated_area', 'area_type': 'area_type', 'comments': 'comments',
        'null_comment': 'null_comment', 'reg_block': 'reg_block', 'block_name': 'block_name',
        'bound_type': 'bound_type', 'feature_type': 'feature_type', 'deed_plan_': 'deed_plan_',
        'county': 'county', 'sub_county': 'sub_county', 'sub_location': 'sub_location',
        'dmp_number': 'dmp_number', 'original_number': 'original_number', 'transaction': 'transaction',
        'old_parcel': 'old_parcel', 'owner': 'owner', 'current_status': 'current_status',
        'shape_leng': 'shape_leng', 'shape_area': 'shape_area', 'dp_number': 'dp_number',
        'power_count': 'power_count', 'water_count': 'water_count', 'building_count': 'building_count',
        'permit_count': 'permit_count', 'address_count': 'address_count', 'uuid': 'uuid',
        'BL_LRNumber': 'BL_LRNumber', 'BL_LR Amount': 'BL_LR Amount', 'BL_LR InvoiceNo': 'BL_LR InvoiceNo',
        'BL_CustomerNo': 'BL_CustomerNo', 'BL_CustomerNo1': 'BL_CustomerNo1', 'BL_KRAPIN': 'BL_KRAPIN',
        'BL_CustomerID': 'BL_CustomerID', 'BL_PhoneNumber': 'BL_PhoneNumber', 'BL_PropertyValue': 'BL_PropertyValue',
        'BL_uuid_derived': 'BL_uuid_derived', 'fusion': 'fusion', 'propertyName': 'propertyName',
        'rates_count': 'rates_count', 'trade_count': 'trade_count'
    }

def _get_power_field_mapping():
    return {
        'geom': 'geom', 'REFERENCE': 'REFERENCE', 'LEGACY_NUMBER': 'LEGACY_NUMBER',
        'CODIGO': 'CODIGO', 'SERIAL_NUM': 'SERIAL_NUM', 'FABRICATION_DATE': 'FABRICATION_DATE',
        'IND_PREPAYMENT': 'IND_PREPAYMENT', 'METER_STATUS': 'METER_STATUS', 'ID_MARK': 'ID_MARK',
        'ID_MODEL': 'ID_MODEL', 'ID_MEASURING_POINT': 'ID_MEASURING_POINT', 'UPDATE_DATE': 'UPDATE_DATE',
        'UPDATE_USER': 'UPDATE_USER', 'NISS': 'NISS', 'ID_SECTOR': 'ID_SECTOR',
        'SERVICE_RATE_TYPE': 'SERVICE_RATE_TYPE', 'SECTOR_SUPPLY_STATUS': 'SECTOR_SUPPLY_STATUS',
        'CONNECT_DATE': 'CONNECT_DATE', 'ID_BILLING_CLASS': 'ID_BILLING_CLASS', 'POT': 'POT',
        'DISCONNECT_NUM': 'DISCONNECT_NUM', 'CLIENT_CODE': 'CLIENT_CODE', 'FIRST_NAME': 'FIRST_NAME',
        'MIDDLE_NAME': 'MIDDLE_NAME', 'LAST_NAME': 'LAST_NAME', 'TELEPHONE_NUM': 'TELEPHONE_NUM',
        'FAX_NUM': 'FAX_NUM', 'OFFICE_TEL': 'OFFICE_TEL', 'EMPLOYER': 'EMPLOYER', 'STAFF_NUMBER': 'STAFF_NUMBER',
        'OCCUPATION': 'OCCUPATION', 'EMAIL': 'EMAIL', 'ID_DEVICE_TYPE': 'ID_DEVICE_TYPE',
        'ACCOUNT_PHASE': 'ACCOUNT_PHASE', 'TIP_TENSION': 'TIP_TENSION', 'CTE_APA': 'CTE_APA',
        'DEVICE_PHASE': 'DEVICE_PHASE', 'CONNECTION_TYPE': 'CONNECTION_TYPE', 'PLACE_TYPE': 'PLACE_TYPE',
        'CONTRACT_STATUS': 'CONTRACT_STATUS', 'CONTRACT_FROM_DATE': 'CONTRACT_FROM_DATE',
        'CONTRACT_END_DATE': 'CONTRACT_END_DATE', 'CONTRACT_CREATION_DATE': 'CONTRACT_CREATION_DATE',
        'SUBSTN_NUM': 'SUBSTN_NUM', 'COD_CONNECTION': 'COD_CONNECTION', 'COD_PREMISE': 'COD_PREMISE',
        'READ_SEQUENCE': 'READ_SEQUENCE', 'METERBOX_V10': 'METERBOX_V10', 'MEASURING_POINT_STATUS': 'MEASURING_POINT_STATUS',
        'ACTION_': 'ACTION_', 'TIP_CONTR': 'TIP_CONTR', 'TARIFF': 'TARIFF',
        'M_GPS_X': 'M_GPS_X', 'M_GPS_Y': 'M_GPS_Y', 'PHASE': 'PHASE',
        'DOC_TYPE': 'DOC_TYPE', 'DOC_NUMBER': 'DOC_NUMBER', 'SEC_NIS': 'SEC_NIS',
        'IMP_TOTAL': 'IMP_TOTAL', 'ROW_NUM': 'ROW_NUM', 'uuid': 'uuid'
    }

def _get_water_field_mapping():
    return {
        'geom': 'geom', 'GM_LAYER': 'GM_LAYER', 'GM_TYPE': 'GM_TYPE',
        'LAYER': 'LAYER', 'IDNO': 'IDNO', 'ACCOUNTNO': 'ACCOUNTNO', 'FIRSTNAME': 'FIRSTNAME',
        'MIDDLENAME': 'MIDDLENAME', 'SURNAME': 'SURNAME', 'IDCARD': 'IDCARD', 'PINNO': 'PINNO',
        'POSTAL': 'POSTAL', 'CITY': 'CITY', 'POSTALCODE': 'POSTALCODE', 'TELEPHONE': 'TELEPHONE',
        'CELLPHONE': 'CELLPHONE', 'METERACCES': 'METERACCES', 'METERSERIA': 'METERSERIA',
        'F_SERIALNO': 'F_SERIALNO', 'METERREADI': 'METERREADI', 'METERMAKE': 'METERMAKE',
        'PLOTNO': 'PLOTNO', 'ESTATENAME': 'ESTATENAME', 'LANDMARK': 'LANDMARK', 'SERVICES': 'SERVICES',
        'METERREMAR': 'METERREMAR', 'GENERAL_RE': 'GENERAL_RE', 'CUST_REMAR': 'CUST_REMAR',
        'PREMISEACC': 'PREMISEACC', 'STREETNAME': 'STREETNAME', 'PREMISETYP': 'PREMISETYP',
        'METERDIALS': 'METERDIALS', 'X_COORD': 'X_COORD', 'Y_COORD': 'Y_COORD',
        'REGION2': 'REGION2', 'uuid': 'uuid'
    }

def _get_dev_control_field_mapping():
    return {
        'geom': 'geom', 'type': 'type', 'sub_county': 'sub_county', 'sub_category': 'sub_category',
        'coordinates': 'coordinates', 'lr_number': 'lr_number', 'parcel_no': 'parcel_no',
        'development_cost': 'development_cost', 'professionals_involved': 'professionals_involved',
        'created_at': 'created_at', 'number': 'number', 'submission_number': 'submission_number',
        'applicant_id': 'applicant_id', 'status': 'status', 'decision': 'decision',
        'is_paid': 'is_paid', 'submitted_at': 'submitted_at', 'decided_at': 'decided_at',
        'form_filled': 'form_filled', 'JSON.ward': 'JSON.ward', 'JSON.acreage': 'JSON.acreage',
        'JSON.owner_id': 'JSON.owner_id', 'JSON.parcel_no': 'JSON.parcel_no', 'JSON.conditions': 'JSON.conditions',
        'JSON.owner_name': 'owner_name', 'JSON.applicant_email': 'applicant_email',
        'JSON.applicant_phone': 'applicant_phone', 'JSON.lr_number_value': 'JSON.lr_number_value',
        'JSON.applicant_id_pin': 'applicant_id_pin', 'JSON.current_land_use': 'JSON.current_land_use',
        'JSON.molpp_issue_date': 'JSON.molpp_issue_date', 'JSON.applicant_address': 'JSON.applicant_address',
        'JSON.relevant_easements': 'JSON.relevant_easements',
        'JSON.development_description': 'development_description',
        'JSON.applicant_other_land_interest': 'JSON.applicant_other_land_interest',
        'JSON.applicant_name': 'applicant_name', 'JSON.land_zone_code': 'JSON.land_zone_code',
        'Longitude': 'Longitude', 'Latitude': 'Latitude', 'uuid': 'uuid'
    }
