from pyspark.sql import Row
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

retail_orders = spark.read.load(
    "../data/retail/orders",
    format="csv",
    sep=",",
    schema="order_id int, order_date string, customer_id int, order_status string"
)


print("Row: make an RDD from a list of Rows")
rows = [
    Row(name="Alice", age=11),
    Row(name="Tom", age=13),
    Row(name="James", age=25),
]

rdd = spark.sparkContext.parallelize(rows)
for i in rdd.collect():
    print(f"{i.name}:\t{i.age}")


print("Row: make a DF from a custom Row class")
Person = Row("Name","Age")
people = [
    Person("Alice", 12),
    Person("Tom", 20),
    Person("James", 5),
]

people_df = spark.createDataFrame(people)
people_df.show(truncate=False)

# Row methods
test = Row(name="Alice", age=11, username="Alice")

print("Row: count()")
print(test.count("Alice"))

print("Row: index()")
print(test.index(11))

print("Row: asDict()")
print(test.asDict())


print("Accessing Columns (3 ways)")
print("df.colname")
retail_orders.select(retail_orders.order_status).show(3)
print('df["colname"]')
retail_orders.select(retail_orders["order_status"]).show(3)
print("F.col(colname)")
retail_orders.select(F.col("order_status")).show(3)


print("Sort by order status (ascending)")
retail_orders.orderBy(retail_orders.order_status.asc()).show(truncate=False)
print("Sort by order status (descending)")
retail_orders.orderBy(retail_orders.order_status.desc()).show(truncate=False)

print ("Get all order Ids between 10 and 20")
retail_orders.where(retail_orders.order_id.between(10, 20)).show(truncate=False)

print("Count orders with status == CLOSED")
print(retail_orders.where(retail_orders.order_status.contains("CLOSED")).count())

print("Count all orders with status == CLOSED or PENDING")
print(retail_orders.where(retail_orders.order_status.isin(["CLOSED","PENDING"])).count())

print("Equality check using == vs eqNullSafe")
df = spark.createDataFrame(
    [
        Row(id=1, value1="foo", value2="foo"),
        Row(id=2, value1=None,  value2=None),
        Row(id=3, value1="bar", value2="foo")
    ]
)
df.withColumn(
    "normal_equality_check",
    F.col("value1") == F.col("value2")
).withColumn(
    "eqNullSafe_check",
    F.col("value1").eqNullSafe(F.col("value2"))
).show(truncate=False)


print("count orders completed in each calendar year")
retail_orders.filter(
    F.col("order_status") == "COMPLETE"
).withColumn(
    "year",
    F.col("order_date").substr(1,4)
).groupby("year").count().show(truncate=False)

print("use when to make short names for order statuses")
retail_orders.select(
    F.col("order_status"),
    F.when(
        F.col("order_status") == "CLOSED", "CL"
    ).when(
        F.col("order_status") == "COMPLETE", "CO"
    ).otherwise("other").alias("order_status2")
).show(10)