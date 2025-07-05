import os
from pathlib import Path
import geopandas as gpd
import numpy as np
from shapely.ops import nearest_points
from dotenv import load_dotenv
import logging
from Geopandax.Utilities import UtilDB
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parent.parent
config_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=config_path)

schema_name = os.getenv('PIPELINE_SILVER_SCHEMA')

def AddUUIDToCadastre(cadastre):
    cadastre["uuid"] = cadastre["parcel_number"].fillna(cadastre["lr_number"])
    cadastre["uuid"] = cadastre["uuid"].fillna(pd.Series(cadastre.index + 1, index=cadastre.index))
    UtilDB.WriteToDB(
        cadastre, "langata_cp_power_water_bld_permit_address_uuid", "AddUUIDToCadastre", "")
    return cadastre

def _process_layer_with_uuid(cadastre, layer_name, output_table, function_name):
    gdf = UtilDB.readFromDB(schema_name, layer_name)
    gdf_joined = gpd.sjoin(gdf, cadastre, how='left', predicate='intersects')
    gdf_joined.drop(columns=[col for col in gdf_joined.columns if col.endswith('_right')], inplace=True)
    gdf_joined.rename(columns={col: col.replace('_left', '') for col in gdf_joined.columns}, inplace=True)

    columns_to_keep = list(gdf.columns) + ["uuid"]
    gdf_joined = gdf_joined[[col for col in gdf_joined.columns if col in columns_to_keep]]

    UtilDB.WriteToDB(gdf_joined, output_table, function_name, "")
    print(f'{function_name} processed: {gdf_joined.shape[0]}')

def KPLC(cadastre):
    _process_layer_with_uuid(cadastre, "langata_power_customers", "langata_power_customers_with_nlims_link", "KPLC")

def NWSC(cadastre):
    _process_layer_with_uuid(cadastre, "langata_water_meters", "langata_water_meters_with_nlims_link", "NWSC")

def BuildingFootPrints(cadastre):
    _process_layer_with_uuid(cadastre, "langata_buildings", "langata_buildings_with_nlims_link", "BuildingFootPrints")

def DevelopmentControl(cadastre):
    _process_layer_with_uuid(cadastre, "langata_building_permits", "langata_building_permits_with_nlims_link", "DevelopmentControl")

def AddressPoints(cadastre):
    _process_layer_with_uuid(cadastre, "langata_address_locations", "langata_address_locations_with_nlims_link", "AddressPoints")

def join_attributes_by_nearest(target_gdf, join_gdf, join_fields):
    target_gdf = target_gdf.to_crs(join_gdf.crs)

    def find_nearest(row):
        nearest_geom = nearest_points(row.geometry, join_gdf.unary_union)[1]
        nearest_row = join_gdf.loc[join_gdf.geometry == nearest_geom]
        return nearest_row[join_fields].iloc[0] if not nearest_row.empty else None

    joined_data = target_gdf.apply(find_nearest, axis=1, result_type='expand')
    return pd.concat([target_gdf.reset_index(drop=True), joined_data.reset_index(drop=True)], axis=1)

def AddressPointsUsingJoinAttributeByNearestParcel(cadastre):
    gdf = UtilDB.readFromDB(schema_name, "langata_address_locations")
    result = join_attributes_by_nearest(gdf, cadastre, ['uuid'])
    UtilDB.WriteToDB(result, "langata_address_locations2_with_nlims_link", "AddressPointsUsingJoinAttributeByNearestParcel", "")

def AddressPointsUsingJoinAttributeByNearestParcel2(cadastre):
    gdf_points = UtilDB.readFromDB(schema_name, "langata_address_locations")
    target_crs = "EPSG:21037"  # UTM Zone 37S (Kenya-specific, units in meters)

    cadastre = cadastre.to_crs(target_crs)
    gdf_points = gdf_points.to_crs(target_crs)
    gdf_nearest = gpd.sjoin_nearest(gdf_points, cadastre, how="left", distance_col="distance")

    columns_to_keep = list(gdf_points.columns) + ["uuid"]
    gdf_nearest = gdf_nearest[[col for col in gdf_nearest.columns if col in columns_to_keep]]

    UtilDB.WriteToDB(gdf_nearest, "langata_address_locations2_with_nlims_link", "AddressPointsUsingJoinAttributeByNearestParcel2", "")
