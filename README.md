# Azure Data Engineering Project – End-to-End Car Sales Data Pipeline

## Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline built using Azure services and the Medallion Architecture (Bronze, Silver, and Gold).

The pipeline ingests raw car sales data from GitHub, loads it into Azure SQL Database using Azure Data Factory, performs incremental data loading into the Bronze layer, and transforms the data using Azure Databricks to build the Silver and Gold layers.

---

## Architecture

```
GitHub (CSV Files)
        │
        ▼
Azure Data Factory (source_prep)
        │
        ▼
Azure SQL Database (Landing Tables)
        │
        ▼
Azure Data Factory (incremental_prep)
        │
        ▼
Stored Procedure + Watermark Table
        │
        ▼
Bronze Layer
        │
        ▼
Azure Databricks
        │
        ▼
Silver Layer
        │
        ▼
Azure Databricks
        │
        ▼
Gold Layer
```

---

## Technologies Used

* Azure Data Factory
* Azure SQL Database
* Azure Databricks
* Delta Lake
* Unity Catalog
* PySpark
* SQL
* GitHub

---

## Project Workflow

### 1. Source Data Ingestion

* Car sales CSV files are stored in GitHub.
* Azure Data Factory (`source_prep`) copies the files into Azure SQL Database landing tables.

### 2. Incremental Loading

The `incremental_prep` pipeline:

* Reads the watermark value from `water_table`.
* Loads only newly added records.
* Executes the `UpdateWATERTABLE` stored procedure.
* Updates the watermark after a successful execution.

### 3. Bronze Layer

* Stores raw data with minimal transformation.
* Serves as the source for downstream processing.

### 4. Silver Layer

* Cleans and validates the Bronze data.
* Removes duplicates.
* Applies business transformations.

### 5. Gold Layer

* Creates analytics-ready tables.
* Produces curated datasets for reporting and dashboards.

---

## Repository Structure

```
Azure-DE-Project-Resources/
│
├── Bronze/
│   ├── AzureDataFactory/
│   ├── SQL/
│   └── README.md
│
├── Silver/
│
├── Gold/
│
└── README.md
```

---

# Azure Databricks Pipeline

![Databricks Pipeline](https://github.com/user-attachments/assets/a9ae63b6-54b3-4205-b0b6-e3435a91701c)

---

# Azure Data Factory Pipelines

### source_prep

Loads raw CSV data from GitHub into Azure SQL Database.

![source\_prep](https://github.com/user-attachments/assets/8e4276f6-1a8f-4a1f-b5e2-e12270c4e3e2)

### incremental_prep

Executes the incremental loading process using a watermark table and stored procedure.

![incremental\_prep](https://github.com/user-attachments/assets/6e9cb1bb-6b8c-4528-ba8d-d50cf6a082b9)

---

## Future Enhancements

* CI/CD using Azure DevOps
* Parameterized pipelines
* Data quality validation
* Monitoring and alerting
* Automated deployment

---

## Acknowledgement

This project was built as part of my learning journey by following the Azure Data Engineering Bootcamp by Ansh Lamba. I recreated and implemented the complete pipeline to gain hands-on experience with Azure Data Factory, Azure SQL Database, Azure Databricks, and the Medallion Architecture.
