## Running the ERITS Pipeline

Follow these steps to build, run, and execute the ERITS geospatial pipeline.

---

### **1. Build and Start the Environment**
Run the following from the project root (where `docker-compose.yml` is located):

```bash
docker compose up -d --build
```

This will:

1. Build the custom Airflow image (with Python, Java, Spark, and geospatial libs).
2. Start PostgreSQL, Redis, and all Airflow services (webserver, scheduler, workers).
3. Initialize Airflow with default admin credentials.

---

### **2. Access the Airflow Web UI**
Open your browser and go to:

```
http://localhost:8085
```

Log in with:
```
Username: airflow
Password: airflow
```
(Or the credentials set in your `.env` file.)

---

### **3. Verify DAG Availability**
- On the Airflow homepage, find the DAG named **`erits_proposed`**.
- If it’s missing, check your `docker-compose.yml` volume mapping for `/opt/airflow/dags` and confirm your DAG file is present.
- Make sure the DAG is **ON** (toggle switch).

---

### **4. Configure Database Connection**
In the Airflow UI:

1. Go to **Admin → Connections**.
2. Click **"+"**.
3. Fill in:
   - **Connection ID:** `erits_postgres`
   - **Connection Type:** Postgres
   - **Host:** `host.docker.internal` (Mac/Windows) or Docker subnet IP (Linux)
   - **Port:** `5432`
   - **Database:** `erits`
   - **Login:** `postgres`
   - **Password:** *(from `.env`)*
   - **Extras:** `{"sslmode": "disable"}`

---

#### **PostgreSQL SSL Modes & When to Use Them**
- **disable** – Turns off SSL entirely. Data is sent in plain text. Use only for local development or completely secure private environments.  
- **allow** – Uses SSL encryption if the server supports it, but falls back to non-SSL if not. No identity verification is done. Suitable for flexible, low-security setups.  
- **prefer** – Tries to use SSL first but will still connect without it if unavailable. This is PostgreSQL’s default and works for most internal setups where encryption is preferred but not mandatory.  
- **require** – Forces SSL encryption but does not verify the server’s certificate or hostname. This is useful in internal networks where you control both ends but still want encrypted traffic.  
- **verify-ca** – Requires SSL and checks that the server certificate is signed by a trusted Certificate Authority (CA), but does not check the hostname. Good for semi-secure environments where the CA’s authenticity is trusted.  
- **verify-full** – The most secure mode. Requires SSL, validates the certificate against a trusted CA, and ensures the server’s hostname matches the certificate exactly. Recommended for production over public networks.  

---

### **5. Trigger the Pipeline**
**From Airflow UI:**
- Open `erits_proposed` → Click **Trigger DAG**.

**From CLI:**
```bash
docker exec -it <airflow-worker-container> airflow dags trigger erits_proposed
```

---

### **6. Monitor Progress**
- View the DAG run in the **Graph** view.
- Task groups (`data_preparation`, `generate_uuid`, `link_uuid`, etc.) will run in sequence.
- Failed tasks appear in **red** – click to see logs.

---

### **7. Check Final Outputs**
- The processed datasets will be stored in the **Gold schema** of your PostgreSQL/PostGIS database.

---
