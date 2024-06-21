from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

retail_orders = spark.read.load(
    "../data/retail/orders",
    format="csv",
    sep=",",
    schema="order_id int, order_date string, customer_id int, order_status string"
)

order_items = spark.read.load(
    "../data/retail/order_items",
    format="csv",
    sep=",",
    schema=" itemID int, orderID int, productID int, quantity int, subtotal float, productPrice float"
)

test_scores = spark.createDataFrame(
    [
        ("Robert", "History", 35,40,40),
        ("Robert", "English", 10,85,62),
        ("Ram", "History", 31,33,29),
        ("Ram", "English", 31,33,91),
        ("Hilary", "History", 45,88,76),
    ], schema="name string, class string, test1 int, test2 int, test3 int"
)

employee_data = spark.createDataFrame(
    [
        (1, 'Robert', 2),
        (2, 'Ria', 3),
        (3, 'James', 5),
        (4, 'Hamilton', 5),
    ], schema="empID int, empName string, managerID int",
)

if False:
    print("make order status lowercase")
    retail_orders.select(F.col("order_status"), F.lower("order_status")).show(3)


    print("Use SQL's stack function")
    df = spark.range(1)
    df.selectExpr(" stack(3, 1,2,3,4,5,6) ").show()

    print("Sort orders by customer ID")
    retail_orders.sort(F.col("customer_id")).show(5)

    print("Sort orders by order status and customer ID (descending)")
    retail_orders.sort(F.col("order_status"), F.col("customer_id").desc()).show(5)

    print("Set Ops")
    df1 = spark.range(10)
    df2 = spark.range(5,15)
    df3 = spark.createDataFrame([
        ('a',1), ('b',2)
    ], schema=('col1 string, col2 int'))
    df4 = spark.createDataFrame([
        (2,'b'),(3,'c')
    ], schema=('col2 int, col1 string'))

    print("union")
    df3.union(df4).show()

    print("unionByName")
    df3.unionByName(df4).show()

    print("intersect")
    df1.intersect(df2).show()

    print("intersectAll")
    df1.intersectAll(df2).show()

    print("exceptAll")
    df1.exceptAll(df2).show()


    print("self joins")
    employee_data.alias('left').join(
        employee_data.alias('right').select(F.col("empID"), F.col("name").alias("managerName")),
        on= F.col("left.managerID") == F.col("right.empID"),
        how="left"
    ).drop(F.col("right.empID")).show()

    print("Agg functions")

    print("\n>> summary")
    order_items.summary().show()
    print("\n>> avg/min/max")
    order_items.select(F.avg(F.col("subtotal")), F.min(F.col("subtotal")), F.max(F.col("subtotal"))).show()

    print("\n >>> collect set/list")
    employee_data.select(
        F.collect_list(F.col("managerID")),
        F.collect_set(F.col("managerID")),
    ).show()

    print("\n>> skew/variance/stddev")
    order_items.select(F.skewness(F.col("subtotal")), F.variance(F.col("subtotal")), F.stddev(F.col("subtotal"))).show()

print("\n\nGroupBy")
order_items.groupby(F.col("productID")).min("quantity","productPrice").show()

order_items.groupby(F.col("productID")).agg(F.max("quantity"),F.avg("productPrice")).show()

print(">> groupby+filter")
order_items.groupby(F.col("productID")).agg(
    F.max("quantity"),F.avg("productPrice").alias("avg_price")
).filter(
    F.col("avg_price") > F.lit(200.0)
).show()

print(">> groupby+pivot")
tmp = test_scores.groupby(F.col("name")).pivot("class").avg("test1")
tmp.show()

print(">> unpivot (2 ways)")
tmp.unpivot("name", ["English","History"], "class", "test1_avg").show()

# can also do this using selectExpr & SQL's stack
tmp.selectExpr("name", "stack(2,'English',English,'History',History) as (class,test1_avg)").show()
