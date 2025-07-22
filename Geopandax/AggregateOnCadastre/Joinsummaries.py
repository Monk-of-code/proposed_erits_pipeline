import os
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import logging
from Geopandax.Utilities import UtilDB


logger = logging.getLogger(__name__) 
logger.setLevel(logging.INFO) 

def JoinAllSummaries():
    """Merges all individual summary tables into final cadastre table using uuid"""
    try:
        schema = os.getenv("PIPELINE_SILVER_SCHEMA")
        if not schema:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting summary table merge by uuid...")

        # Load base cadastre (with uuid and geometry)
        logger.info("Loading base cadastre geometry...")
        gdf = UtilDB.readFromDB(schema, "langata_cp_cadastral_parcel_uuid")

        if gdf.empty:
            raise ValueError("Base cadastre table is empty")

        summaries = [
            ("power_summary", "power_count"),
            ("water_summary", "water_count"),
            ("building_summary", "building_count"),
            ("permit_summary", "permit_count"),
            ("address_summary", "address_count"),
        ]

        # Keep only original cadastre columns
        cadastre_cols = list(gdf.columns)
        merged = gdf.copy()
        logger.info(f"Base cadastre shape: {merged.shape}")

        for table, column in summaries:
            try:
                logger.info(f"Merging {table} ({column})...")
                summary_df = UtilDB.readFromDB(schema, table)

                if summary_df.empty:
                    logger.warning(f"{table} is empty — skipping.")
                    continue

                if column not in summary_df.columns:
                    logger.warning(f"{column} missing in {table} — skipping merge.")
                    continue
                if "uuid" not in summary_df.columns:
                    logger.warning(f"'uuid' missing in {table} — cannot merge.")
                    continue

                # Drop any unwanted columns (keep only uuid + count)
                summary_df = summary_df[["uuid", column]]

                # Merge on uuid, avoid geometry or other col pollution
                merged = merged.merge(
                    summary_df,
                    how="left",
                    on="uuid",
                    suffixes=('', '_drop')  # in case duplicate column names
                )

                # Remove any accidentally added columns
                merged = merged[[col for col in merged.columns if not col.endswith('_drop')]]

                # Fill NA with 0 for the count column
                merged[column] = merged[column].fillna(0).astype(int)
                logger.info(f"{column} merged. Non-zero count: {(merged[column] > 0).sum()}")

            except Exception as e:
                logger.error(f"Failed to merge {table}: {str(e)}", exc_info=True)
                raise

        # Remove duplicate UUIDs just in case
        before_dedup = len(merged)
        merged = merged.drop_duplicates(subset=["uuid"], keep="first")
        after_dedup = len(merged)
        logger.warning(f"Removed {before_dedup - after_dedup} duplicate UUID rows.")

        # Write final table
        logger.info("Writing merged cadastre summary table...")
        UtilDB.WriteToDB(
            merged,
            "langata_cp_power_water_bld_permits_address_agg_uuid",  # target table name
            "JoinAllSummaries",
            {"geometry": "geom"}
        )

        logger.info(f"Successfully joined all summaries. Final row count: {len(merged)}")
        logger.info(f"Columns in output: {list(merged.columns)}")

    except Exception as e:
        logger.error(f"Failed to join summaries: {str(e)}", exc_info=True)
        raise

