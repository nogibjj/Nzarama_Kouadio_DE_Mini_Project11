from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum
import re

# Initialize Spark session
spark = SparkSession.builder.appName("TransformData").getOrCreate()

# Clean the dataset
def load_data(spark, local_path):
    """
    Load data from a local CSV file into a PySpark DataFrame, clean up column names, and remove duplicate columns.
    """
    df = spark.read.csv(local_path, header=True, inferSchema=True)

    # Clean column names
    for column_name in df.columns:
        new_col_name = re.sub(r"[^a-zA-Z0-9_]", "", column_name)
        df = df.withColumnRenamed(column_name, new_col_name)

    # Remove duplicate columns
    column_set = set()
    duplicate_columns = [
        col for col in df.columns if col in column_set or column_set.add(col)
    ]
    if duplicate_columns:
        print(f"Duplicate Columns Detected: {duplicate_columns}")
        for col in duplicate_columns:
            df = df.drop(col)

    return df

# Perform Transformation: Creating a winner column for each game and calculating the point difference
def transform_data(df):
    """
    Add a winner column to the DataFrame.
    """
    df = df.withColumn("PointDifference", df["TeamPoints"] - df["OpponentPoints"])
    df = df.withColumn(
        "Winner", when(df["PointDifference"] > 0, df["Team"]).otherwise(df["Opponent"])
    )

    winner_sample = df.select(
        "Game",
        "Team",
        "Opponent",
        "TeamPoints",
        "OpponentPoints",
        "PointDifference",
        "Winner",
    ).limit(10)
    return winner_sample
    
# Function: Write to Data Sink
def write_data(df, output_path):
    """Save the transformed DataFrame to a CSV file."""
    df.write.csv(output_path, header=True, mode="overwrite")
    print(f"Transformed data saved to {output_path}")

# Define the Pipeline
def pipeline(spark, input_path, output_path):
    """End-to-end data pipeline."""
    # Step 1: Load data
    df = load_data(spark, input_path)

    # Step 2: Transform data
    df_transformed = transform_data(df)

    # Step 3: Write data to sink
    write_data(df_transformed, output_path)

# Execute the Pipeline
if __name__ == "__main__":
    input_path = "/workspaces/Nzarama_Kouadio_DE_Mini_Project11/dataset/nba_games_stats.csv"
    output_path = "./transformed_output"

    pipeline(spark, input_path, output_path)
