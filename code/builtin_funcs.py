from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()