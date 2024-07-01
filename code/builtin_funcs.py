from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

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


print("Generate a monotonically increasing ID")
company.withColumn(
    "id",
    F.monotonically_increasing_id(),
).show(truncate=False)


print("Use lit to concat age & salary columns")
company.withColumn(
    "salary|age",
    F.concat(F.col("salary"), F.lit("|"), F.col("age")),
).show(truncate=False)

print("Get name length via sql expression")
company.withColumn(
    "name_len",
    F.expr("length(empname)"),
).show(truncate=False)

print("Generate random columns")
company.withColumn(
    "rand(0)",
    F.rand(0),
).withColumn(
    "randn(0)",
    F.randn(0),
).show(truncate=False)


print("Encrypt employee names")
company.withColumn(
    "sha1",
    F.sha1(F.col("empname")),
).withColumn(
    "sha2(0)",
    F.sha2(F.col("empname"), 0),
).withColumn(
    "hash(empname + age)",
    F.hash(F.col("empname"), F.col("age")),
).select(F.col("empname"), F.col("sha1"), F.col("sha2(0)"), F.col("hash(empname + age)")).show(truncate=False)

print("Combine when and regex to conditionally replace string patterns")
addr = [
    ("2625 Indian School Rd","Phoenix"),
    ("1234 Thomas St", "Glendale")
]
addr_df = spark.createDataFrame(addr, ["street","city"])

addr_df.withColumn(
    "new_street",
    F.when(
        addr_df.street.endswith('Rd'), F.regexp_replace(addr_df.street, 'Rd','Road')
    ).when(
        addr_df.street.endswith('St'), F.regexp_replace(addr_df.street, 'St','Street')
    ).otherwise(addr_df.street)
).show(truncate=False)

print("See if a pattern is found in a string column")
company.select(
    company.empname,
    company.empname.rlike('[Ll]i')
).show()