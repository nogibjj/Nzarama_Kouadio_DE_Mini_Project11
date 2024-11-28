# Test script for cluster setup

# Import SparkSession (used in Databricks)
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestCluster").getOrCreate()

# Test DataFrame
data = [("Les Miserables", "Victor Hugo"), ("L'etranger", "Albert Camus"), ("Le Petit Prince", "Antoine de Saint-Exupéry")]
columns = ["Book", "Author"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Save DataFrame as a CSV to test write functionality
df.write.csv("/tmp/test_output", header=True, mode="overwrite")

print("Cluster and basic setup test completed!")

