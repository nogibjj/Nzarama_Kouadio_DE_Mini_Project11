# Nzarama_Kouadio_DE_Mini_Project11

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
- **File Location**: `/FileStore/tables/nba_games_stats.csv`.

## **2. Transform**
- **Description**: The extracted data undergoes the following transformations:
  1. Clean column names to ensure consistency.
  2. Calculate the point difference between teams (`PointDifference` column).
  3. Identify the winner of each game (`Winner` column).
  4. Classify games as home or away (`HomeGame` and `AwayGame` columns).

## **3. Load**
- **Description**: The transformed data is saved as a CSV file to the Databricks FileStore for further use.
- **File Location**: `/FileStore/tables/transformed_output`.

# Project Structure

```plaintext
.
├── dataset/
│   └── nba_games_stats.csv      # Data source: basketball game statistics
├── project/
│   ├── pipeline.py              # Main ETL pipeline script
│   ├── test_cluster.py          # Test script for Databricks cluster setup
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

# Code Implementation

### **Extract**
```python
def load_data(spark, file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    # Clean column names
    for column_name in df.columns:
        new_col_name = re.sub(r"[^a-zA-Z0-9_]", "", column_name)
        df = df.withColumnRenamed(column_name, new_col_name)
    return df
```

### **Transform**
```python
def transform_data(df):
    df = df.withColumn("PointDifference", df["TeamPoints"] - df["OpponentPoints"])
    df = df.withColumn(
        "Winner", when(df["PointDifference"] > 0, df["Team"]).otherwise(df["Opponent"])
    )
    df = df.withColumn("HomeGame", when(df["Home"] == "Home", 1).otherwise(0))
    df = df.withColumn("AwayGame", when(df["Home"] == "Away", 1).otherwise(0))
    return df
```

### **Load**
```python
def write_data(df, output_path):
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Transformed data saved to {output_path}")
```

### **Pipeline**
```python
def pipeline(spark, input_path, output_path):
    df = load_data(spark, input_path)
    df_transformed = transform_data(df)
    write_data(df_transformed, output_path)
```


# Output Examples



# Cluster Settings
- **Cluster Name**: Nzarama Kouadio's Cluster
- **Runtime**: Databricks Runtime 16.0.x-cpu-ml-scala2.12
- **Driver Type**: Standard_DS3_v2
- **Worker Type**: Standard_DS3_v2

