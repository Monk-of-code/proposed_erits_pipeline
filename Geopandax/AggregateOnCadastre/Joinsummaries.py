import os
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import logging
from Geopandax.Utilities import UtilDB

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=BASE_DIR / ".env")

def JoinAllSummaries():
    """Merges all individual summary tables into final cadastre table without fusion scoring"""
    try:
        schema = os.getenv("PIPELINE_SILVER_SCHEMA")
        if not schema:
            raise ValueError("PIPELINE_SILVER_SCHEMA environment variable not set")

        logger.info("Starting summary table merge (without fusion scoring)...")

        # Load base cadastre
        logger.info("Loading base cadastre geometry...")
        gdf = UtilDB.readFromDB(schema, "langata_cp_cadastral_parcel")

        if gdf.empty:
            raise ValueError("Base cadastre table is empty")

        # List of (table_name, column_name) pairs
        summaries = [
            ("tmp_power_summary", "power_count"),
            ("tmp_water_summary", "water_count"),
            ("tmp_building_summary", "building_count"),
            ("tmp_permit_summary", "permit_count"),
            ("tmp_address_summary", "address_count"),
        ]

        # Initialize with base geometry
        merged = gdf.copy()
        logger.info(f"Base cadastre shape: {merged.shape}")

        for table, column in summaries:
            try:
                logger.info(f"Merging {table} ({column})...")
                summary_df = UtilDB.readFromDB(schema, table)
                logger.info(f"{table} columns: {summary_df.columns.tolist()}")

                if summary_df.empty:
                    logger.warning(f"{table} is empty — skipping.")
                    continue

                # Check required columns
                if column not in summary_df.columns:
                    logger.warning(f"{column} missing in {table} — skipping merge.")
                    continue
                if "index_right" not in summary_df.columns:
                    logger.warning(f"'index_right' missing in {table} — merge will likely fail.")
                    continue

                # Select only necessary columns
                cols_to_merge = ["index_right", column]
                if "geometry" in summary_df.columns:
                    cols_to_merge.append("geometry")

                # Perform merge
                merged = merged.merge(
                    summary_df[cols_to_merge],
                    how="left",
                    left_index=True,
                    right_on="index_right"
                )

                # Drop merge key
                merged.drop(columns=["index_right"], inplace=True, errors="ignore")

                # Fill NA with 0 for the count column
                if column in merged.columns:
                    merged[column] = merged[column].fillna(0).astype(int)
                    logger.info(f"{column} merged. Non-zero count: {(merged[column] > 0).sum()}")
                else:
                    logger.warning(f"{column} missing after merge!")

                logger.info(f"{table} merge complete. Merged shape: {merged.shape}")

            except Exception as e:
                logger.error(f"Failed to merge {table}: {str(e)}", exc_info=True)
                raise

        # Write final output
        logger.info("Writing merged table to database...")
        UtilDB.WriteToDB(
            merged,
            "langata_cp_power_water_bld_permit_address",
            "JoinAllSummaries",
            {"geometry": "geom"}
        )

        logger.info(f"Successfully joined all summaries. Final row count: {len(merged)}")
        logger.info(f"Columns in output: {list(merged.columns)}")

    except Exception as e:
        logger.error(f"Failed to join summaries: {str(e)}", exc_info=True)
        raise
