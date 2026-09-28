# azure_project
# Azure Data Engineering Project – End-to-End Medallion Architecture

## Project Overview

This project demonstrates an **Azure Data Engineering solution** built using **Azure Data Factory, Azure SQL Database, ADLS Gen2, Azure Databricks, Unity Catalog, PySpark, Spark SQL, Databricks Asset Bundles, Lakeflow Declarative Pipelines, Git, and CI/CD**.

The solution handles data ingestion, orchestration, transformation, data quality, incremental processing, Slowly Changing Dimensions, monitoring, and environment-based deployment.

---

## Architecture

```text
External API
     ↓
Azure Data Factory
     ↓
Azure SQL Database
     ↓
Azure Data Factory
     ↓
ADLS Gen2 - Bronze
     ↓
Azure Databricks
     ↓
Silver Layer
     ↓
Gold Layer
```

ADF is also integrated with **Azure Logic Apps** to send pipeline success and failure email notifications.



<img width="1672" height="941" alt="ChatGPT Image Sep 28, 2026, 08_01_57 PM" src="https://github.com/user-attachments/assets/4e91413d-1c77-45f8-b8bb-d27dc9c7292c" />


---

# 1. API Data Ingestion using Azure Data Factory

Data is extracted from an external REST API using **Azure Data Factory** and initially loaded into **Azure SQL Database**.

```text
External API
     ↓
Azure Data Factory
     ↓
Azure SQL Database
```

ADF acts as the main orchestration service for the ingestion process.

<img width="1002" height="696" alt="Screenshot 2026-09-26 at 12 18 28 AM" src="https://github.com/user-attachments/assets/1b73d1ad-7e58-4882-af9d-0bc9eac762f9" />


---

# 2. Azure SQL to ADLS Gen2

ADF is used to move data from Azure SQL Database into the **Bronze layer of ADLS Gen2**.

The pipeline uses:

- **Copy Activity** for data movement
- **ForEach Activity** for processing multiple tables
- **Script Activity** for SQL operations
- **Web Activity** for triggering Logic Apps

This allows the ingestion process to handle multiple datasets through reusable pipeline logic.

---

# 3. Pipeline Monitoring and Email Notifications

ADF **Web Activity** is integrated with an **Azure Logic App** to send email notifications when a pipeline succeeds or fails.

Notifications can contain information such as:

```text
Pipeline Name
Run ID
Execution Status
Failure Details
```
<img width="1723" height="913" alt="screenshot" src="https://github.com/user-attachments/assets/6406d366-d9b9-4d2f-bd52-864d566d5f91" />

This provides better monitoring and makes pipeline failures easier to troubleshoot.

---

# 4. Git and CI/CD

Azure Data Factory is integrated with **Git** for source control and CI/CD.

This provides:

- Version control
- Change tracking
- Collaboration
- Controlled deployments
- Development and production separation

```text
Development
    ↓
Git
    ↓
CI/CD
    ↓
Production
```

---

# 5. ADLS Gen2 Bronze Layer

Raw data is stored in the **Bronze layer** of ADLS Gen2.

Datasets used in the project include:

```text
DimUser
DimArtist
DimTrack
DimDate
FactStream
```

The Bronze layer serves as the source for Databricks processing.

---

# 6. Azure Databricks and Unity Catalog

Azure Databricks accesses data from ADLS Gen2 using **Unity Catalog**.

Unity Catalog is used to organize and manage catalogs, schemas, volumes, and tables.

The Bronze data is accessed through Unity Catalog volumes and processed into managed Delta tables.

For example, `DimUser` is read incrementally from the Bronze layer using Auto Loader and Structured Streaming.

---

# 7. Databricks Asset Bundles

**Databricks Asset Bundles** are used to manage and deploy Databricks resources across separate environments.

```text
DEV
 ↓
Testing
 ↓
PROD
```

Asset Bundles provide a consistent deployment approach for notebooks, jobs, pipelines, and other Databricks resources.

---

# 8. Medallion Architecture

The Databricks implementation follows the **Medallion Architecture**:

```text
Bronze → Silver → Gold
```

- **Bronze:** Raw source data
- **Silver:** Cleaned, transformed, and deduplicated data
- **Gold:** Business-ready tables with incremental processing and SCD logic

---

# 9. Silver Layer Transformations

The Silver layer uses **PySpark, Spark SQL, Auto Loader, Structured Streaming, and Delta Lake**.

Main transformations include:

- Removing duplicate records
- Cleaning `_rescued_data`
- Standardizing text fields
- Creating derived columns
- Handling schema evolution


---

# 10. Auto Loader and Incremental Processing using declarative pipelines

Databricks **Auto Loader** is used to incrementally process files arriving in the Bronze layer.

The pipelines use:

```python
spark.readStream.format("cloudFiles")
```

along with schema locations, schema evolution, and checkpointing.

<img width="1093" height="685" alt="Screenshot 2026-09-28 at 7 34 30 PM" src="https://github.com/user-attachments/assets/86ce41bd-0214-4276-8c00-f4b581afe2dc" />



---

# 11. Delta Lake

Silver-layer data is stored using **Delta Lake**.

The pipelines use features such as:

```
Structured Streaming
Checkpointing
Schema Evolution
Delta Tables
```

This provides reliable incremental processing and supports downstream Gold-layer pipelines.

---

# 12. Gold Layer using Lakeflow Declarative Pipelines

The Gold layer is built using **Databricks Lakeflow Declarative Pipelines** and Auto CDC.

Silver Delta tables are used as streaming sources for the Gold layer.

```text
Silver Table
     ↓
Streaming Table
     ↓
Auto CDC
     ↓
Gold Table
```

---

# 13. Slowly Changing Dimensions

The Gold layer uses **SCD Type 2** for dimension tables to preserve historical changes.

The following dimensions use SCD Type 2:

```
DimUser
DimArtist
DimTrack
DimDate
```

Auto CDC is implemented using:

```python
dlt.create_auto_cdc_flow()
```

For example, `DimArtist` uses `artist_id` as the key and `updated_at` as the sequence column.

`DimUser` also applies a data-quality rule to reject records where `user_id` is null before SCD processing.

---

# 14. FactStream Processing

`FactStream` uses **SCD Type 1** rather than SCD Type 2.

Its configuration uses:


The pipeline processes FactStream incrementally using Auto CDC.

Therefore:


Dimension Tables → SCD Type 2
FactStream       → SCD Type 1


---

# 15. Data Quality

Data-quality checks are applied during Silver and Gold processing.

The project includes:

- Duplicate removal
- Null validation
- Schema evolution
- Data standardization
- Business transformations
- CDC processing

For example, `DimUser` requires `user_id` to be non-null before records are loaded into the SCD2 target.

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Azure Data Factory | Data ingestion and orchestration |
| Azure SQL Database | Initial staging |
| ADLS Gen2 | Data lake storage |
| Azure Logic Apps | Email notifications |
| Azure Databricks | Data processing |
| Unity Catalog | Data governance |
| PySpark | Data transformations |
| Spark SQL | Data querying |
| Databricks Auto Loader | Incremental file ingestion |
| Structured Streaming | Incremental processing |
| Delta Lake | Table storage |
| Lakeflow Declarative Pipelines | Gold-layer processing |
| Auto CDC | Change processing |
| Databricks Asset Bundles | DEV/PROD deployment |
| Git | Source control |
| CI/CD | Automated deployment |

