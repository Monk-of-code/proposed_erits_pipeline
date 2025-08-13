
###  DAG Structure & Task Groups

#### **Parquet Handling**
- Writes the **raw cadastre table** to Parquet.
- Writes the **UUID-assigned cadastre table** to Parquet.
- Purpose: Provide quick local copies for downstream tasks to read without hitting the database repeatedly.

#### **Data Preparation**
- Prepares each feature layer:
  - **Cadastre** – filters to active parcels in Lang’ata.
  - **Water** – filters water meter connections in Lang’ata.
  - **Development** – filters development control permits.
  - **Address** – filters address point locations.
  - **Buildings** – filters building footprint data.
  - **Power** – filters electricity meter connections.
- Each step:
  - Reads raw data from the database.
  - Performs a **spatial join** with the Lang’ata boundary.
  - Cleans column names and removes extra join columns.
  - Writes the cleaned dataset back to the Silver schema.

#### **Generate UUID**
- Assigns a unique ID to each cadastre parcel:
  - Uses `parcel_number` if present.
  - Falls back to `lr_number` if missing.
  - If both are missing, generates a synthetic `uuid_X`.
- Cleans duplicate UUIDs.
- Writes output to `langata_cp_cadastral_parcel_uuid`.

#### **Link UUID**
- Spatially links features to the cadastre UUID:
  - Power, Water, Buildings, Development permits.
- Joins by intersection and keeps only relevant fields.
- Writes linked datasets to Silver schema tables.

#### **Address Preparation**
- Links addresses to cadastre parcels using:
  - **Nearest neighbor** method (GeoPandas `sjoin_nearest` – default).
  - **Fallback shapely nearest points** method if needed.
- Ensures every address is assigned a parcel UUID.

#### **Aggregates**
- Counts how many features fall within each parcel for:
  - Power meters
  - Water meters
  - Buildings
  - Addresses
  - Development permits
- Outputs summary tables with UUID, geometry, and feature count.

#### **Join Aggregates**
- Merges all summary tables into a **single enriched cadastre table**.
- Fills missing counts with 0.
- Adds a **fusion score** (0–5) based on presence of features.
- Adds property names from address data.

#### **Interest**
- Joins **ownership, land rate, and trade license data** with the enriched cadastre.
- Identifies parcels with potential declared interests.
- Calculates counts for land rate records and trade licenses.

#### **Postprocessing**
- Removes duplicate UUIDs.
- Drops non-essential fields, keeping only the required production attributes.

#### **Move to Gold**
- Migrates cleaned, enriched datasets to the Gold schema:
  - Cadastre with full enrichment.
  - Linked Power and Water customer datasets.
  - Linked Development permit datasets.

---

### **3. Execution Flow**
The DAG follows this sequence:
1. **Test Database Connection** → Ensures Postgres is reachable before starting.
2. **Initialization** → Loads Lang’ata boundary and stores it in Parquet.
3. **Data Preparation** → Filters and cleans feature datasets.
4. **Write Cadastre Parquet** → Stores cadastre in Parquet for quick reuse.
5. **Generate UUID** → Assigns unique parcel identifiers.
6. **Write UUID Parquet** → Stores UUID cadastre in Parquet.
7. **Link UUID & Address Prep** → Attaches UUID to all features and addresses.
8. **Aggregate Features** → Counts features per parcel.
9. **Join Aggregates** → Merges counts into enriched cadastre.
10. **Interest Enrichment** → Adds ownership, land rate, and license data.
11. **Postprocess** → Deduplicates and cleans final table.
12. **Move to Gold** → Migrates results to production-ready schema.

---

### **4. Error Handling**
- All tasks have a **global failure callback**:
  - Logs failing task ID.
  - Can be extended for email or Slack alerts.
- Tasks raise `AirflowException` on errors to trigger retries.

---

### **5. Best Practices in This DAG**
- **Task Groups** – Keep UI clean and group related processes logically.
- **Parquet Exchange** – Avoids repeated DB queries, speeds up tasks.
- **Environment Variables** – Config-driven approach avoids hardcoding.
- **Modular Imports** – All feature processing logic is in reusable Python modules.
- **UUID Linking** – Maintains data integrity and traceability across datasets.

---

### **6. Outputs**
By the end of the DAG:
- The **Gold schema** contains:
  - Fully enriched cadastre with UUIDs, counts, fusion score, ownership, and economic activity indicators.
- All intermediate Silver tables and Parquet files remain available for debugging or incremental updates.

---
