
# Mini Project 11: Data Pipeline Using Databricks

# Project Overview

This project demonstrates the creation of an **ETL (Extract, Transform, Load) pipeline** using **Databricks**. The goal of the pipeline is to:
1. Extract data from a **data source** (a CSV file).
2. Perform transformations to clean and enrich the data.
3. Load the processed data into a **data sink** for storage and further use.

### **Key Concepts**
- **Data Source**: The starting point of the pipeline where raw data is stored (in this case, the CSV file `nba_games_stats.csv`).
- **Data Transformations**: Intermediate steps where the raw data is cleaned, structured, and enriched.
- **Data Sink**: The final destination for the processed data (in this project, a transformed CSV file stored in the Databricks FileStore).
- **Data Pipeline**: The entire process that connects the data source, transformations, and the data sink.

# Pipeline Steps

## **1. Extract**
- **Description**: Data is extracted from the CSV file `nba_games_stats.csv`.
- **Implementation**: The file is read into a Spark DataFrame using the `spark.read.csv()` function.
- **File Location**: `dbfs:/FileStore/tables/nmk_43_pipeline/nba_games_stats.csv`.

## **2. Transform**
- **Description**: The extracted data undergoes the following transformations:
  1. Clean column names to ensure consistency.
  2. Calculate the point difference between teams (`PointDifference` column).
  3. Identify the winner of each game (`Winner` column).
  4. Classify games as home or away (`HomeGame` and `AwayGame` columns).

## **3. Load**
- **Description**: The transformed data is saved as a CSV file to the Databricks FileStore for further use.
- **File Location**: `dbfs:/FileStore/tables/nmk_43_pipeline/transformed_output`.

# Project Structure

```plaintext
.
├── dataset/
│   └── nba_games_stats.csv      # Data source: basketball game statistics
├── project/
│   ├── pipeline.py              # Main ETL pipeline script
│   └── transformed_output/      # Folder for processed data output
├── README.md                    # Project documentation
├── Makefile                     # Makefile for CI/CD or setup tasks
├── requirements.txt             # Python dependencies
└── setup.sh                     # Shell script for environment setup
```

# How to Run the Pipeline

### **1. Prerequisites**
- A running Databricks cluster with access to the Databricks FileStore.

### **2. Steps to Execute**
1. **Upload the Dataset**:
   - Navigate to the **Data** tab in Databricks.
   - Upload the file `nba_games_stats.csv` to the FileStore at `/FileStore/tables/`.

2. **Run the Notebook**:
   - Create a notebook in Databricks and copy the `pipeline.py` code.
   - Attach the notebook to your cluster.
   - Run the notebook cells step by step.

3. **Verify the Output**:
   - Transformed data is saved to `/FileStore/tables/transformed_output`.

# Proof of Pipeline Success 

1. Proof of script running smoothly and file being saved 
[Success Run](/workspaces/Nzarama_Kouadio_DE_Mini_Project11/Pic_Proof/run_success.png)

2. Proof of storage
[Storage Success](/workspaces/Nzarama_Kouadio_DE_Mini_Project11/Pic_Proof/storage.png)

# Cluster Settings
- **Cluster Name**: Nzarama Kouadio's Cluster
- **Runtime**: Databricks Runtime 16.0.x-cpu-ml-scala2.12
- **Driver Type**: Standard_DS3_v2
- **Worker Type**: Standard_DS3_v2

