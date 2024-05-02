from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName("Example").getOrCreate()

print("Created")

spark.stop()