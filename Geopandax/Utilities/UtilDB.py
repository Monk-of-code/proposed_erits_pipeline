import os
from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.future import select
from dotenv import load_dotenv

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Load environment variables
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(dotenv_path=BASE_DIR / '.env')

# SQLAlchemy Base
Base = declarative_base()


def get_db_config():
    return {
        "host": os.getenv('PIPELINE_HOST_IP'),
        "port": os.getenv('PIPELINE_HOST_PORT'),
        "database": os.getenv('PIPELINE_HOST_NAME'),
        "user": os.getenv('PIPELINE_HOST_USER'),
        "password": os.getenv('PIPELINE_HOST_PASSWORD'),
        "silver_schema": os.getenv('PIPELINE_SILVER_SCHEMA'),
        "bronze_schema": os.getenv('PIPELINE_BRONZE_SCHEMA'),
    }


def create_db_engine(async_=False):
    config = get_db_config()
    prefix = "postgresql+asyncpg" if async_ else "postgresql"
    return create_engine(
        f"{prefix}://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    )


#def WriteToDB(dataFrame, outputTable, functionName, toPostGISOption):
    engine = create_db_engine()
    schema = get_db_config()['silver_schema']
    if_exists = 'append' if toPostGISOption else 'replace'
    dataFrame.to_postgis(name=outputTable, schema=schema, con=engine, if_exists=if_exists)
    print(f'{outputTable} ({functionName}) Records written to DB: {dataFrame.shape[0]}')

from Geopandax.Utilities.UtilDB import create_db_engine, get_db_config
from sqlalchemy import inspect, text
import pandas as pd

def WriteToDB(dataFrame, outputTable, functionName, toPostGISOption):
    engine = create_db_engine()
    schema = get_db_config()['silver_schema']
    if_exists = 'append' if toPostGISOption else 'replace'

    inspector = inspect(engine)
    table_exists = inspector.has_table(outputTable, schema=schema)

    if table_exists and toPostGISOption:
        existing_columns = [col['name'] for col in inspector.get_columns(outputTable, schema=schema)]
        new_columns = [col for col in dataFrame.columns if col not in existing_columns and col != dataFrame.geometry.name]

        with engine.begin() as conn:
            for col in new_columns:
                col_type = _map_dtype(dataFrame[col])
                print(f'[ALTER] Adding column "{col}" as {col_type}')
                alter_stmt = text(f'ALTER TABLE "{schema}"."{outputTable}" ADD COLUMN "{col}" {col_type};')
                conn.execute(alter_stmt)

    # Write to PostGIS
    dataFrame.to_postgis(
        name=outputTable,
        schema=schema,
        con=engine,
        if_exists=if_exists,
        index=False
    )
    print(f'{outputTable} ({functionName}) Records written: {dataFrame.shape[0]}')


def _map_dtype(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    else:
        return 'TEXT'


def WriteNonSpatialToDB(dataFrame, outputTable, functionName):
    engine = create_db_engine()
    schema = get_db_config()['silver_schema']
    dataFrame.to_sql(name=outputTable, con=engine, schema=schema, if_exists="replace", index=False)
    print(f'{outputTable} ({functionName}) Records written to DB: {dataFrame.shape[0]}')


def readFromDB(schema_name, table_name):
    engine = create_db_engine()
    query = f'SELECT * FROM "{schema_name}"."{table_name}"'
    return gpd.read_postgis(query, con=engine, geom_col="geom")


def readNonSpatialFromDB(schema_name, table_name):
    engine = create_db_engine()
    query = f'SELECT * FROM "{schema_name}"."{table_name}"'
    return pd.read_sql(query, con=engine)


def CopyFromTableAtoTableB(source_table, target_table, schema_name, field_mapping):
    engine = create_db_engine()
    metadata = MetaData(schema=schema_name)
    metadata.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    source = Table(source_table, metadata, autoload_with=engine)
    target = Table(target_table, metadata, autoload_with=engine)

    session.execute(delete(target))

    source_fields = list(field_mapping.keys())
    source_data = session.query(*[source.c[field] for field in source_fields]).all()

    for row in source_data:
        row_dict = dict(zip(source_fields, row))
        mapped_data = {target_field: row_dict[source_field] for source_field, target_field in field_mapping.items()}
        session.execute(target.insert().values(**mapped_data))

    session.commit()


async def copy_from_table_a_to_table_b(source_table_name, target_table_name, schema_name, field_mapping):
    config = get_db_config()
    connection_string = f"postgresql+asyncpg://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    engine = create_async_engine(connection_string, echo=True)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with engine.begin() as conn:
            metadata = MetaData(schema=schema_name)
            await conn.run_sync(metadata.reflect)
            source = Table(source_table_name, metadata, autoload_with=conn.sync_engine)
            target = Table(target_table_name, metadata, autoload_with=conn.sync_engine)

            await session.execute(delete(target))

            source_fields = list(field_mapping.keys())
            result = await session.execute(select(*[source.c[field] for field in source_fields]))
            for row in result.fetchall():
                row_dict = dict(zip(source_fields, row))
                mapped_data = {target_field: row_dict[source_field] for source_field, target_field in field_mapping.items()}
                await session.execute(target.insert().values(**mapped_data))
            await session.commit()


def readFromDBWithTimestamp(schema_name, table_name, last_processed_time):
    try:
        if isinstance(last_processed_time, datetime):
            last_processed_time = last_processed_time.strftime('%Y-%m-%d %H:%M:%S')

        engine = create_db_engine()
        query = f"""
        SELECT * FROM "{schema_name}"."{table_name}"
        WHERE updated_at_langata > '{last_processed_time}' OR "{table_name}".updated_at_langata IS NULL
        """
        return gpd.read_postgis(query, con=engine, geom_col="geom")

    except Exception as e:
        print(f"Error reading from {schema_name}.{table_name}: {e}")
        return None


def UpdateTableTimestamp(Table):
    config = get_db_config()
    engine = create_db_engine()
    schema = config['silver_schema']
    Session = sessionmaker(bind=engine)
    session = Session()
    now = datetime.now()

    try:
        session.query(Table).filter(Table.updated_at_langata < now).update(
            {Table.updated_at_langata: now}, synchronize_session=False
        )
        session.commit()
        print(f"{Table.__tablename__} updated_at_langata field updated to {now}.")
    except Exception as e:
        session.rollback()
        print(f"Error updating {Table.__tablename__}: {e}")
    finally:
        session.close()


def migrate_table(
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    drop_if_exists: bool = True,
    postgis: bool = True
):
    engine = create_db_engine()

    with engine.begin() as conn:
        inspector = inspect(engine)

        if drop_if_exists and inspector.has_table(target_table, schema=target_schema):
            print(f"[INFO] Dropping existing table: {target_schema}.{target_table}")
            conn.execute(text(f'DROP TABLE "{target_schema}"."{target_table}"'))

        print(f"[INFO] Reading from source: {source_schema}.{source_table}")
        sql = f'SELECT * FROM "{source_schema}"."{source_table}"'

        if postgis:
            try:
                gdf = gpd.read_postgis(sql, conn, geom_col="geom")
                print(f"[INFO] Read {len(gdf)} rows")
            except Exception as e:
                print(f"[ERROR] Failed to read PostGIS table: {e}")
                return
        else:
            df = pd.read_sql(sql, conn)
            print(f"[INFO] Read {len(df)} rows")

        print(f"[INFO] Writing to target: {target_schema}.{target_table}")
        try:
            if postgis:
                gdf.to_postgis(target_table, conn, schema=target_schema, if_exists="replace", index=False)
            else:
                df.to_sql(target_table, conn, schema=target_schema, if_exists="replace", index=False)
        except Exception as e:
            print(f"[ERROR] Failed to write table: {e}")
            return

    print(f"[DONE] Migrated: {source_schema}.{source_table} ➜ {target_schema}.{target_table}")