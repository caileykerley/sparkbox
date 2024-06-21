from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.master('local[1]').appName("Example").getOrCreate()

# udf
print("\n>>> write a udf that title-cases strings")

## udf with dataframe
@F.udf(returnType=StringType())
def all_caps(phrase: str) -> str:
    return phrase.title()
df = spark.createDataFrame([("francis bacon", 2), ("napoleon bonaparte", 45)], schema="name: string, count: int")
df.select(df.name, all_caps(df.name)).show()

## udf with spark sql
spark.udf.register("all_caps", all_caps)
spark.sql("""select all_caps("hello world")""").show()
# spark.sql("""select name,all_caps(name) new_name from table""").show()

print("\n>>> write a udf that gets string length")
slen = F.udf(lambda x: len(x), IntegerType())
spark.udf.register("slen", slen)
spark.sql("""select slen("hello world")""").show()


# read
print("\n>>> read a CSV")
retail_orders = spark.read.load(
    "../data/retail/orders",
    format="csv",
    sep=",",
    schema="order_id int, order_date string, customer_id int, order_status string"
)

retail_orders.show(5, truncate=False)

spark.stop()