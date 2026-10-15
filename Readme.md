# ERITS Geospatial Pipeline: Setup and Implementation Guide

- [ ] Repository: `e_pipeline_proposed` | Airflow DAG: `erits_proposed` | Study area: Lang'ata sub-county, Nairobi, Kenya

This guide is written from a full read of the repository code. It explains what the pipeline does, how to stand it up from scratch, what data it expects, what it produces, and where the repository's own README and sample config disagree with the code. Section 11 lists those discrepancies with fixes, so read it before your first run.

---

## 1. What the pipeline does

The pipeline takes raw land and utility layers for Kenya and produces one enriched record per land parcel in Lang'ata. Its steps are:

1. Clip every input layer (parcels, power meters, water meters, buildings, building permits, addresses) to the Lang'ata boundary.
2. Give each cadastral parcel a unique ID (`uuid`).
3. Link every utility, building, permit and address point to the parcel it sits on.
4. Count how many of each feature fall on each parcel.
5. Compute a **fusion score** (0 to 5): the number of feature types present on the parcel. The README describes it as an indicator of likely rental activity.
6. Attach property names from address data, land-rate billing records and trade-license counts.
7. Deduplicate, trim columns and publish the results to a **Gold** schema in PostGIS.

It runs as one Apache Airflow DAG, triggered manually, on a Docker Compose stack (CeleryExecutor, Redis broker).

## 2. Architecture

### 2.1 Components

| Component | Role |
| --- | --- |
| Apache Airflow 2.10.5 (Python 3.12 image) | Orchestrates the DAG: webserver, scheduler, Celery worker |
| Compose `postgres` (v15) | Airflow's **metadata** database only (host port 5435). It does not hold your geospatial data |
| Redis 7.2 | Celery broker |
| **External PostgreSQL + PostGIS** | Holds the Bronze, Silver and Gold schemas. You must provide this yourself |
| GeoPandas, Shapely, GeoAlchemy2, SQLAlchemy | All spatial processing and DB I/O in the DAG |
| Parquet files in `./data` | Local cache shared between tasks |
| PySpark + Apache Sedona | Experimental batch spatial join for buildings, not wired into the DAG (section 10) |

### 2.2 Data layers (medallion)

| Schema | Contents |
| --- | --- |
| `Bronze` (`PIPELINE_BRONZE_SCHEMA`) | Raw, province or country wide source tables you load beforehand |
| `Silver` (`PIPELINE_SILVER_SCHEMA`) | Lang'ata-clipped layers, UUID-linked layers, per-parcel summaries, the enriched cadastre working table |
| `Gold` (`PIPELINE_GOLD_SCHEMA`) | Final published tables |

Schema names are quoted in every query, so they are case-sensitive. `Bronze` and `bronze` are different schemas.

### 2.3 Execution flow

```
test_postgres_connection
  -> initialize (boundary -> Silver + g_selected.parquet)
  -> data_preparation: cadastre | water | development | address | buildings | power   (parallel)
  -> parquet_handling: write_cadastre_parquet
  -> generate_uuid: add_uuid_to_cadastre
  -> parquet_handling: write_cadastreUUID_parquet
  -> link_uuid: link_kplc | link_nwsc | link_buildings | link_development   (parallel)
     address_preparation: link_address_intersect | link_address_nearest      (parallel with link_uuid)
  -> aggregates: power | water | buildings | address | development          (parallel)
  -> join_aggregates: join_summaries -> add_fusion -> add_propertyname
  -> interest: potential_interest_to_cadastre -> aggregate_landrates -> aggregate_trade_license -> join_fields_landrate
  -> postprocessing: drop_duplicates -> clear_unwanted_rows
  -> move_to_gold: cadastre | development | power | water                  (parallel)
```

DAG settings: `schedule_interval=None` (manual trigger only), `catchup=False`, 2 retries with a 5 minute delay per task, tags `erits` and `geospatial`. A global failure callback prints the failed task ID.

## 3. Repository layout

```
e_pipeline_proposed/
├── Dockerfile                 Airflow image + GDAL/GEOS/PROJ/Java 17 + Python deps
├── docker-compose.yml         Airflow stack (webserver, scheduler, worker, init, postgres, redis)
├── requirements.txt           Python dependencies
├── sample.env                 Template for .env
├── Readme.md / How_to_run.md  Original overview and run notes
└── Geopandax/
    ├── airflow/dags/eirts-v3.py        The DAG (dag_id: erits_proposed)
    ├── DataPrep/                       Clip layers to Lang'ata (+ BuildingsPrepSpark.py)
    ├── CadastreUUID/                   UUID generation and layer-to-parcel linking
    ├── AggregateOnCadastre/            Per-parcel counts, join, fusion score
    ├── Interest/                       Land-rate / trade-license enrichment, cleanup, Gold move
    ├── Utilities/UtilDB.py             DB read/write helpers (GeoPandas path)
    ├── Utilities/UtilDBSpark.py        Spark JDBC helpers
    └── Data_Preparation.py             Older non-Airflow entry point (stale, see section 11)
```

In the containers the repo is mounted at `/opt/airflow/erits-pipeline` with `PYTHONPATH` pointing there, so `from Geopandax...` imports work. The DAG folder is mounted at `/opt/airflow/dags/erits_dags`.

## 4. Prerequisites

- Docker Engine and Docker Compose v2, with enough RAM for GeoPandas spatial joins (8 GB minimum; more for large buildings layers).
- A reachable **PostgreSQL with the PostGIS extension**, containing the `Bronze`, `Silver` and `Gold` schemas (section 6, step 2).
- Source data loaded into Bronze (section 5).
- Network path from the Airflow containers to the PostGIS host.

## 5. Input data contract (Bronze schema)

Every spatial table must keep its geometry in a column named **`geom`**, because the read helper hard-codes `geom_col="geom"`. All spatial layers must share the same CRS, since the joins do not reproject (the one exception is the nearest-parcel address join, which reprojects to EPSG:21037).

| Bronze table | Needed columns | Used for |
| --- | --- | --- |
| `au_subcounty_unit` | `scouname`, `geom` | Boundary. Row where `scouname = 'Lang''ata'` is selected |
| `cp_cadastral_parcel` | `current_status`, `parcel_number`, `lr_number`, `geom` | Parcels. Only `current_status = 'Active'` is kept |
| `power_customers` | `geom` | Power meters |
| `water_meters` | `geom` | Water meters |
| `building_permits` | `geom` (points) | Development control permits |
| `address` | `propertyna`, `geom` | Address points, property names |
| `langata_buildings` | `updated_at_langata`, `geom` | Building footprints (already Lang'ata-sized) |
| `bd_land_rate_data` | `uuid_derived`, `KRA PIN`, `Customer ID`, `Customer No`, `Customer No1`, `LRNumber`, `LR Amount`, `LR Invoice No`, `Phone Number`, `Property Value` | Land-rate billing records, matched to parcels via `uuid_derived` |
| `trade_license` | `uuid`, `geom` | Trade licenses, counted per parcel via `uuid` |

Two of these are easy to miss:

- `uuid_derived` and `trade_license.uuid` must already use the same ID scheme this pipeline generates (`parcel_number`, falling back to `lr_number`). The pipeline does not compute them.
- `langata_buildings.updated_at_langata` is required because the building aggregation filters on it.

## 6. Setup, step by step

### Step 1: Get the code

```bash
unzip e_pipeline_proposed-main.zip
cd e_pipeline_proposed-main
```

### Step 2: Prepare the PostGIS database

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE SCHEMA IF NOT EXISTS "Bronze";
CREATE SCHEMA IF NOT EXISTS "Silver";
CREATE SCHEMA IF NOT EXISTS "Gold";
```

Load the Bronze tables from section 5 (for example with `ogr2ogr` or `shp2pgsql`), making sure the geometry column is called `geom`.

### Step 3: Create `.env`

Copy the template to the repo root, the same folder as `docker-compose.yml`:

```bash
cp sample.env .env
```

Edit it. The template contains placeholders such as `{POSTGRES_HOST}`; replace each one **including the braces and the stray space** after `PIPELINE_HOST_PORT=`. A working example:

```env
AIRFLOW_PROJ_DIR=${PWD}
AIRFLOW_UID=1000                       # run `id -u` on Linux; see section 11

PIPELINE_HOST_IP=host.docker.internal  # or the DB server's reachable IP
PIPELINE_HOST_PORT=5432
PIPELINE_HOST_NAME=erits
PIPELINE_HOST_USER=postgres
PIPELINE_HOST_PASSWORD=your_password

PIPELINE_BRONZE_SCHEMA=Bronze
PIPELINE_SILVER_SCHEMA=Silver
PIPELINE_GOLD_SCHEMA=Gold

PARQUET_FOLDER=/opt/airflow/data
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=choose_a_strong_password
```

If the password contains characters such as `@`, `/` or `:`, the code builds the connection URL without URL-encoding, so either avoid those characters or percent-encode them.

**Linux hosts:** `host.docker.internal` is not defined by default. Either use the database server's real IP, or add this under `x-airflow-common` in `docker-compose.yml`:

```yaml
  extra_hosts:
    - "host.docker.internal:host-gateway"
```

### Step 4: Check dependencies

`requirements.txt` lists geopandas, shapely, fiona, pyproj, rtree, psycopg2-binary, apache-sedona, python-dotenv, pyspark==3.5.1 and geoalchemy2. The DAG writes and reads Parquet through GeoPandas, which needs **`pyarrow`**; it is not listed. Add a line `pyarrow` to `requirements.txt` before building unless you have confirmed it is already in the image.

### Step 5: Build and start

```bash
docker compose up -d --build
```

The `airflow-init` container migrates the Airflow metadata DB and creates the admin user, then the webserver, scheduler and worker start. Check with `docker compose ps`; all long-running services should be healthy.

### Step 6: Open Airflow

Go to `http://localhost:8085` and log in with the `_AIRFLOW_WWW_USER_*` values from `.env` (default `airflow` / `airflow`; change the password).

### Step 7: Create the Airflow connection

The `test_postgres_connection` task uses an Airflow connection with ID `erits_postgres`. In **Admin > Connections > +**:

| Field | Value |
| --- | --- |
| Connection Id | `erits_postgres` |
| Connection Type | Postgres |
| Host | same as `PIPELINE_HOST_IP` |
| Port | same as `PIPELINE_HOST_PORT` |
| Schema (database) | same as `PIPELINE_HOST_NAME` |
| Login / Password | same as `PIPELINE_HOST_USER` / `PIPELINE_HOST_PASSWORD` |
| Extra | `{"sslmode": "disable"}` (use `require` or `verify-full` outside a private network) |

This connection is only used for the preflight check. All data processing reads the `PIPELINE_HOST_*` variables instead, so **both must be correct**.

### Step 8: Run

DAGs are created paused. In the UI, unpause `erits_proposed` and click **Trigger DAG**. Or from the CLI:

```bash
docker compose exec airflow-scheduler airflow dags unpause erits_proposed
docker compose exec airflow-scheduler airflow dags trigger erits_proposed
```

If the DAG does not appear, check the Import Errors banner in the UI and confirm that `Geopandax/airflow/dags/eirts-v3.py` is visible at `/opt/airflow/dags/erits_dags` inside the container.

### Step 9: Verify output

```sql
SELECT COUNT(*), MIN(fusion), MAX(fusion)
FROM "Gold".langata_cp_kplc_nwsc_bldg_permits_address_gold;
```

## 7. Configuration reference

| Variable | Used by code? | Meaning |
| --- | --- | --- |
| `PIPELINE_HOST_IP`, `_PORT`, `_NAME`, `_USER`, `_PASSWORD` | Yes | PostGIS connection for all data work |
| `PIPELINE_BRONZE_SCHEMA`, `_SILVER_`, `_GOLD_` | Yes | Schema names |
| `PARQUET_FOLDER` | Yes | Parquet cache directory (default `/opt/airflow/data`, mapped to `./data`) |
| `AIRFLOW_PROJ_DIR` | Yes (compose) | Host path mounted into containers |
| `AIRFLOW_UID` | Yes (compose) | Container user ID |
| `_AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` | Yes (compose) | Airflow admin login |
| `AIRFLOW_CONN_erits_postgres` | Probably not | See section 11, item 3 |
| `USE_Spark` / `USE_SPARK` | No effect | See section 11, item 1 |
| `SPARK_*` | Only by the standalone Spark script | Spark tuning |
| `RETRY_COUNT`, `RETRY_DELAY_SECONDS`, `NOTIFY_EMAIL`, `NOTIFY_ON_FAILURE` | No | Defined in the template but never read; the DAG hard-codes retries |

## 8. Task-by-task walkthrough

### 8.1 Initialize

`initialize` reads `Bronze.au_subcounty_unit`, keeps the row with `scouname = "Lang'ata"`, writes it to `Silver.langata_au_subcounty_unit` and to `data/g_selected.parquet`. All prep tasks load that Parquet file as the clipping boundary.

### 8.2 Data preparation

Each task reads its Bronze table, runs an inner spatial join (`intersects`) with the boundary, drops the join's `_right` columns, restores `_left` names, keeps only the original columns, and **replaces** a Silver table.

| Task | Bronze source | Silver output |
| --- | --- | --- |
| `cadastre` | `cp_cadastral_parcel` (Active only) | `langata_cp_cadastral_parcel` |
| `water` | `water_meters` | `langata_water_meters` |
| `development` | `building_permits` | `langata_building_permits` |
| `address` | `address` | `langata_address_locations` |
| `buildings` | `langata_buildings` | `langata_buildings` |
| `power` | `power_customers` | `langata_power_customers` |

### 8.3 Parquet handling

`write_cadastre_parquet` dumps `Silver.langata_cp_cadastral_parcel` to `data/cadastre.parquet`. `write_cadastreUUID_parquet` dumps the UUID table to `data/cadastreUUID.parquet`. Downstream link tasks read these files instead of querying the database again.

### 8.4 Generate UUID

`add_uuid_to_cadastre` sets `uuid` = `parcel_number`, else `lr_number`, else a synthetic `uuid_<row number>`. It trims whitespace, then **drops duplicate UUIDs (keep first)** and writes `Silver.langata_cp_cadastral_parcel_uuid`. Be aware that two parcels sharing a `parcel_number` collapse into one row.

### 8.5 Link UUID

Each layer is left-joined to the UUID cadastre (`intersects`) and keeps its original columns plus `uuid`.

| Task | Input | Output |
| --- | --- | --- |
| `link_kplc` | `langata_power_customers` | `langata_power_customers_with_nlims_link` |
| `link_nwsc` | `langata_water_meters` | `langata_water_meters_with_nlims_link` |
| `link_buildings` | `langata_buildings` | `langata_buildings_with_nlims_link` |
| `link_development` | `langata_building_permits` | `langata_building_permits_with_nlims_link` |

### 8.6 Address preparation

- `link_address_intersect`: `intersects` join, output `langata_address_locations_with_nlims_link`.
- `link_address_nearest`: reprojects points and parcels to EPSG:21037 (Arc 1960 / UTM 37S, metres) and runs `sjoin_nearest`, so every address gets a parcel, output `langata_address_locations2_with_nlims_link`. See section 11, item 6 before relying on this output.

### 8.7 Aggregates

Each task joins its layer to `langata_cp_cadastral_parcel_uuid` and counts features per `uuid`:

| Task | Predicate | Silver table | Count column |
| --- | --- | --- | --- |
| `power` | intersects | `power_summary` | `power_count` |
| `water` | intersects | `water_summary` | `water_count` |
| `buildings` | intersects (rows with `updated_at_langata` after 2025-02-04 or null) | `building_summary` | `building_count` |
| `address` | intersects | `address_summary` | `address_count` |
| `development` | **within** | `permit_summary` | `permit_count` |

These tables hold only parcels with at least one feature (right merge).

### 8.8 Join aggregates

1. `join_summaries` left-joins the five count columns onto the UUID cadastre (missing becomes 0) and writes `Silver.langata_cp_power_water_bld_permits_address_agg_uuid`, the main working table.
2. `add_fusion` adds `fusion` = number of the five counts that are greater than 0 (range 0 to 5).
3. `add_propertyname` joins address points to parcels and adds up to three distinct names as `property_name_1` to `property_name_3`.

### 8.9 Interest enrichment

These four tasks all read and rewrite the working table.

- `potential_interest_to_cadastre`: joins `Bronze.bd_land_rate_data` on `uuid`, keeps parcels where `KRA PIN`, `Customer ID` and `Customer No` are all present. Writes `Silver.langata_potential_interests_to_land` (spatial) and `Silver.potential_interests_to_cadastre` (non-spatial, `BL_` prefixed columns).
- `aggregate_landrates`: adds `rates_count`, the number of land-rate records per parcel.
- `aggregate_trade_license`: adds `trade_count` from `Bronze.trade_license`.
- `join_fields_landrate`: joins the land-rate columns onto the cadastre with `BL_` prefixes (`BL_KRAPIN`, `BL_CustomerID`, `BL_PhoneNumber`, `BL_PropertyValue`, and so on) and sets `owner` to an empty string.

### 8.10 Postprocessing

`drop_duplicates` enforces one row per `uuid`. `clear_unwanted_rows` keeps a fixed list of production columns (parcel attributes, the five counts, `fusion`, `property_name_1..3`, `rates_count`, `trade_count`, and the `BL_*` fields); columns missing from the table are skipped with a warning.

### 8.11 Move to Gold

`migrate_table` drops the target and rewrites it from Silver.

| Gold table | Silver source |
| --- | --- |
| `langata_cp_kplc_nwsc_bldg_permits_address_gold` | `langata_cp_power_water_bld_permits_address_agg_uuid` |
| `langata_power_customers_with_nlims_link_gold` | `langata_power_customers_with_nlims_link` |
| `langata_water_meters_with_nlims_link_gold` | `langata_water_meters_with_nlims_link` |
| `langata_building_permits_with_nlims_link_gold` | `langata_building_permits_with_nlims_link` |

The linked buildings and address tables stay in Silver.

## 9. Output: the enriched parcel table

One row per parcel, with geometry in `geom`:

- **Identity:** `uuid`, `lr_number`, `parcel_number`, `fid`, `sheet_number`, `reg_block`, and the other cadastral attributes in the keep list.
- **Counts:** `power_count`, `water_count`, `building_count`, `permit_count`, `address_count`, `rates_count`, `trade_count`.
- **Fusion score:** `fusion`, 0 to 5. Higher means more independent signals (power, water, building, permit, address) coincide on the parcel.
- **Names:** `property_name_1` to `property_name_3`.
- **Billing:** `BL_*` fields from land-rate data.

The score is a simple presence count with equal weights. It is not a trained model, so treat it as a screening indicator and validate against ground truth before using it for enforcement or revenue decisions.

## 10. Spark mode

The README says the pipeline can run on Spark. In the code, Spark support is limited to a standalone script, `Geopandax/DataPrep/BuildingsPrepSpark.py`, that is **not called by the DAG**. It reads `Bronze."183_buildings"` in batches of 100,000 rows, joins to the sub-county with Sedona `ST_Intersects`, writes per-batch tables and unions them into `Silver.langatas_buildings`. The DAG's `buildings` task always uses the GeoPandas path and reads `langata_buildings`. Treat Spark as experimental and out of the supported flow. It also assumes a different schema (`id`, a WKT `geometry` column, `subcounty_name`) from the GeoPandas tables, and `UtilDBSpark.py` hard-codes a driver IP.

## 11. Known issues and discrepancies (verified in code)

| # | Issue | Effect | Fix |
| --- | --- | --- | --- |
| 1 | `sample.env` sets `USE_Spark=true`; the code reads `USE_SPARK` and compares to `"1"` | Spark switch never activates; DAG is GeoPandas-only regardless | Leave Spark off; ignore the variable |
| 2 | `sample.env` has `AIRFLOW_UUID`; compose reads `AIRFLOW_UID` (no default in the init `chown`) | Init can fail or permissions break | Set `AIRFLOW_UID` (`id -u` on Linux, `50000` otherwise) |
| 3 | `AIRFLOW_CONN_erits_postgres` is lowercase; Airflow looks up env connections as `AIRFLOW_CONN_` plus the upper-cased ID | Connection likely not found by `test_postgres_connection` | Create it in the UI (step 7) or rename to `AIRFLOW_CONN_ERITS_POSTGRES` |
| 4 | `pyarrow` missing from `requirements.txt` | Parquet read/write may fail | Add `pyarrow` |
| 5 | Aggregate tasks and `JoinAllSummaries` pass a non-empty dict as the 4th argument to `WriteToDB`, which means **append** | Re-runs stack duplicate rows; `drop_duplicates(keep first)` then keeps the oldest, possibly stale, counts | Before any re-run drop the Silver summary tables and the working table (section 12), or change those calls to pass `""` |
| 6 | `link_address_nearest` is fed `load_cadastre()` (raw parcels, no `uuid`) rather than `load_cadastre_uuid()` | Output table `langata_address_locations2_with_nlims_link` likely has no `uuid` | Pass `load_cadastre_uuid()` in the DAG |
| 7 | Building aggregation hard-codes `2025-02-04` and needs `updated_at_langata`; on any error the helper returns `None`, then `.empty` raises | Task fails with an `AttributeError` that hides the real SQL error | Make sure the column exists, parameterise the date, or remove the filter |
| 8 | `migrate_table` catches read/write errors, prints them and returns | A failed Gold move shows green in Airflow | Raise the exception instead; verify Gold row counts after each run |
| 9 | README mentions ownership records; code only joins land-rate data and sets `owner = ""` | `owner` is always blank | Add an ownership join if needed |
| 10 | `Data_Preparation.py` and `CadastreUUIDMain.py` are an older sequential path: `AddUUIDToCadastre` returns `None`, empty-summary branches pass an invalid `if_exists=` argument | Do not use them; the DAG is the only working entry point | Run through Airflow |
| 11 | Left joins in the link step duplicate a feature that touches several parcels | Slightly inflated linked tables | Dedupe or use `within`/centroid if exact one-to-one is needed |
| 12 | Compose mounts `/var/run/docker.sock` and ships `airflow`/`airflow` defaults | Security exposure | Remove the socket mount if unused; set a strong password |
| 13 | Parquet is shared via the `./data` bind mount | Works on one Docker host only | Use shared storage if you add workers on other machines |

## 12. Operations

### Monitor

Use the Graph view in the UI; red tasks have logs under the task instance. From the CLI: `docker compose logs -f airflow-worker`.

### Clean re-run

Because of item 5, drop these before triggering again:

```sql
DROP TABLE IF EXISTS "Silver".power_summary, "Silver".water_summary,
  "Silver".building_summary, "Silver".permit_summary, "Silver".address_summary,
  "Silver".langata_cp_power_water_bld_permits_address_agg_uuid;
```

All other Silver and Gold tables are replaced each run.

### Run a single stage

Clear a task in the UI to re-run it and its downstream tasks. Later stages read Silver tables, so earlier outputs must still exist.

### Stop and reset

`docker compose down` stops the stack. `docker compose down -v` also wipes the Airflow metadata volume; your PostGIS data is untouched.

## 13. Adapting to another sub-county

1. In `initialize()` (in `eirts-v3.py`), change the `"Lang'ata"` filter value.
2. Table names are prefixed `langata_` throughout `DataPrep`, `CadastreUUID`, `AggregateOnCadastre`, `Interest` and the DAG. Rename consistently, or leave the prefix and accept the misleading name.
3. Review the `updated_at_langata` references in `UtilDB.readFromDBWithTimestamp` and `AggregateBuildings`.
4. Confirm your `uuid_derived` and `trade_license.uuid` values follow your area's ID scheme.

## 14. Troubleshooting

| Issue | Likely cause | Action |
| --- | --- | --- |
| DAG missing | Import error or volume mismatch | Check the Import Errors banner; `docker compose exec airflow-scheduler ls /opt/airflow/dags/erits_dags` |
| `ModuleNotFoundError: Geopandax` | `PYTHONPATH` or mount broken | Confirm `AIRFLOW_PROJ_DIR` and the `/opt/airflow/erits-pipeline` mount |
| `test_postgres_connection` fails | Connection missing or host unreachable | Recheck step 7; on Linux fix `host.docker.internal` |
| `schema "Bronze" does not exist` | Case or missing schema | Create schemas with exact case; match `.env` |
| `column "geom" does not exist` or geometry error | Geometry column not named `geom` | Rename in Bronze |
| Parquet error | `pyarrow` absent or `data/` not writable | Add `pyarrow`; check `AIRFLOW_UID` ownership of `./data` |
| Duplicate or stale counts after re-run | Append behaviour (item 5) | Run the clean re-run SQL |
| Worker killed or very slow | Memory pressure on large joins | Raise Docker memory; lower worker concurrency |
| `join_fields_landrate` fails on `KRA PIN` | `bd_land_rate_data` columns differ | Match the column names in section 5 |

## 15. To be implemented 

1. Fix items 1 to 8 in section 11 (all small changes).
2. Replace the `airflow`/`airflow` credentials and move secrets out of `.env` into Airflow connections or a secrets backend.
3. Add row-count assertions after each stage and after the Gold move.
4. Wire a real alert in `failure_callback` (the compose file already contains a commented SMTP block).
5. Decide whether the Spark path stays; if so, connect it to the DAG and align its schema, otherwise remove it and the unused variables.
