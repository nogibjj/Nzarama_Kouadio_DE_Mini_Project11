# Test script for cluster setup

# Import SparkSession (used in Databricks)
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TestCluster").getOrCreate()

# Test DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Save DataFrame as a CSV to test write functionality
df.write.csv("/tmp/test_output", header=True)

print("Cluster and basic setup test completed!")
