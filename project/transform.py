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

# Perform Transformations
# First Transformation: Creating a winner column for each game and calculating the point difference
def declare_winner(df):
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

# Second Transformation: Calculate the Games Played by Each Team
def calculate_games(df):
    """
    Calculate the total games played by each team.
    """
    df = df.withColumn("HomeGame", when(df["Home"] == "Home", 1).otherwise(0))
    df = df.withColumn("AwayGame", when(df["Home"] == "Away", 1).otherwise(0))

    total_games = df.groupBy("Team").agg(
        sum("HomeGame").alias("HomeGames"),
        sum("AwayGame").alias("AwayGames"),
        (sum("HomeGame") + sum("AwayGame")).alias("TotalGames"),
    )

    games_summary_df = total_games.select(
        "Team", "HomeGames", "AwayGames", "TotalGames"
    ).orderBy("TotalGames", ascending=False)

    return games_summary_df

# Main Execution
if __name__ == "__main__":
    # Load the data
    local_path = "/workspaces/Nzarama_Kouadio_DE_Mini_Project11/dataset/nba_games_stats.csv"
    df = load_data(spark, local_path)

    # Apply transformations
    winner_sample = declare_winner(df)
    games_summary = calculate_games(df)

    # Show results
    print("Winner Transformation Sample:")
    winner_sample.show()

    print("Games Summary:")
    games_summary.show()

    # Save Transformed Data
    df.write.csv("./transformed_output", header=True, mode="overwrite")
    print("Transformed data saved to ./transformed_output")
