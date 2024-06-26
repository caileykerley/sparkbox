from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

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

data = [
    ("James", "Sales", "NY", 9000, 34),
    ("Alicia", "Sales", "NY", 8600, 56),
    ("Robert", "Sales", "CA", 8100, 30),
    ("John", "Sales", "AZ", 8600, 31),
    ("Ross", "Sales", "AZ", 8100, 33),
    ("Lisa", "Finance", "CA", 9000, 24),
    ("Deja", "Finance", "CA", 9900, 40),
    ("Sugie", "Finance", "NY", 8300, 36),
    ("Ram", "Finance", "NY", 7900, 53),
    ("Kyle", "Marketing", "CA", 8000, 25),
    ("Reid", "Marketing", "NY", 9100, 50),
]
company = spark.createDataFrame(data,schema=['empname','dept','state','salary','age'])

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

print("\n\nWindow API")

print("\n>>Ranking")
print("Get salary ranking within each department")
spec = Window.partitionBy("dept").orderBy(df.salary.desc())
company.withColumn(
    "dept_salary_row_num", F.row_number().over(spec)
).withColumn(
    "dept_salary_rank", F.rank().over(spec)
).withColumn(
    "dept_salary_dense_rank", F.dense_rank().over(spec)
).select(
    F.col("dept"), F.col("salary"), F.col("dept_salary_row_num"), F.col("dept_salary_rank"), F.col("dept_salary_dense_rank")
).show(truncate=False)

print("Salary percentiles & ntiles")
company.withColumn(
    "percentile_rank",
    F.percent_rank().over(spec)
).withColumn(
    "4_ntile_rank",
    F.ntile(4).over(spec)
).withColumn(
    "cumulative_dist",
    F.cume_dist().over(spec)
).select(
    F.col("dept"), F.col("salary"), F.col("percentile_rank"), F.col("4_ntile_rank"), F.col("cumulative_dist")
).show(truncate=False)

print("\n>> Analytical")
spec = Window.partitionBy("dept").orderBy(df.salary.desc())

company.withColumn(
    "lag_salary",
    F.lag("salary").over(spec)
).withColumn(
    "lead_salary",
    F.lead("salary",2,0).over(spec) # optional vals give offset amount and fill NULL ends
).select(
    F.col("dept"), F.col("salary"), F.col("lag_salary"), F.col("lead_salary")
).show()

print("\n>> Aggregate")
spec = Window.partitionBy("dept")

print("Per-department stats")
company.withColumn(
    "max_salary",
    F.max("salary").over(spec)
).withColumn(
    "min_salary",
    F.min("salary").over(spec)
).withColumn(
    "sum_salaries",
    F.sum("salary").over(spec)
).withColumn(
    "employee_count",
    F.count("empname").over(spec)
).withColumn(
    "avg_age",
    F.avg("age").over(spec)
).show(truncate=False)

print("\nFirst & Last")
spec = Window.partitionBy("dept").orderBy("age")
company.withColumn(
    "smallest_age",
    F.first("age").over(spec)
).withColumn(
    "largest_age",
    F.last("age").over(spec)
).show(truncate=False)

print("\n>> rangeBetween/rowsBetween (unbounded)")
range_spec_u = Window.partitionBy("dept").orderBy("salary").rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
row_spec_u = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# when un-bounded, this is basically just a normal sum
company.select(
    company.dept, company.salary
).withColumn(
    "sum_salary_range",
    F.sum("salary").over(range_spec_u)
).withColumn(
    "sum_salary_row",
    F.sum("salary").over(row_spec_u)
).show(truncate=False)

print("\n>> rangeBetween (bounded to (0,500)) /rowsBetween (bounded to (0,1))")
range_spec_u = Window.partitionBy("dept").orderBy("salary").rangeBetween(Window.currentRow, 500)
row_spec_u = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.currentRow, 1)

company.select(
    company.dept, company.salary
).withColumn(
    "sum_salary_range",
    F.sum("salary").over(range_spec_u)
).withColumn(
    "sum_salary_row",
    F.sum("salary").over(row_spec_u) # when bounded, this is a local (row) sum
).show(truncate=False)

print("\n\nSampling")

print("Get a 10% sample with replacement")
spark.range(100).sample(withReplacement=True, fraction=0.1,seed=42).show()

print("\nStratified sample - by state")
company.groupby(company.state).count().show()
company.sampleBy(company.state, fractions={'NY':0.5,'AZ':0.5,'CA':0.5},seed=0).show()

print("\n\n>> Other Agg Functions")

company.select(
    F.first(company.salary),
).show()

company.select(
    F.greatest(company.salary, company.age),
).show()

company.select(
    F.least(company.salary, company.age),
).show()

company.select(
    F.skewness(company.salary),
).show()

company.groupby(F.col("state")).agg(
    F.skewness(company.salary),
).show()

company.select(
    F.collect_list(company.age),
).show(truncate=False)