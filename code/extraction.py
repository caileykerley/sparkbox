from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

orders = spark.read.load(
    "../data/retail/orders",
    format="csv",
    sep=",",
    schema="order_id int, order_date string, customer_id int, order_status string"
)

print(">> calculate num orders per status type & save to 4 csv files")
tmp = orders.groupby("order_status").count().repartition(4)
tmp.write.save(
    "../output_csv",
    format="csv",
    mode="overwrite",
    header=True
)


print(">> save order status counts as a single compressed txt file")
tmp.select(
    F.concat_ws("\t",F.col("order_status"), F.col("count"))
).coalesce(1).write.save(
    "../output_txt",
    format="text",
    mode="overwrite",
    compression="gzip",
)

print(">> save orders as parquet, partitioned by order_status")
orders.write.save(
    "../output_parquet",
    format="parquet",
    mode="overwrite",
    partitionBy="order_status"
)