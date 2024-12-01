from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestCluster").getOrCreate()

# Read the CSV file as the data source
df = spark.read.csv("/workspaces/Nzarama_Kouadio_DE_Mini_Project11/dataset/nba_games_stats.csv", header=True, inferSchema=True)

# Show the DataFrame to verify
df.show()

# Save DataFrame as a CSV to test write functionality
df.write.csv("./test_cluster_output", header=True, mode="overwrite")

print("Cluster and basic setup test completed!")

