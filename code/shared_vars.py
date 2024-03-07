from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

print(">>> broadcast vars")
days = {"sun": "Sunday", "tue": "Tuesday"}
bc_days = spark.sparkContext.broadcast(days)

data = [
    (1, "sun"),
    (2, "tue"),
    (3, "sun"),
]
rdd = spark.sparkContext.parallelize(data)

def convert(abbrev):
    return days[abbrev]


print(rdd.map(lambda x: (x[0], convert(x[1]))).collect())


print(">>> accumulator vars")
counter = spark.sparkContext.accumulator(0)

rdd.foreach(lambda x: counter.add(x[0]))
print(counter.value)
