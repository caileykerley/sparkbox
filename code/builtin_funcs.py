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

class_data = spark.createDataFrame(
    [
        ('Alicia', ['Java', 'Scala', 'Spark'], {'hair': 'black', 'eye': 'brown'}),
        ('Robert', ['Java', 'Spark', 'Java'], {'hair': 'brown', 'eye': None}),
        ('Mike', ['C#'], {'hair': 'red', 'eye': ''}),
        ('John', None, None),
        ('Jeff', [None, '1', '2'], {}),
    ],
    ['first_name','languages','properties']
)

test_scores = spark.createDataFrame(
    [
        ('John', [10, 20, 25], [25, 11, 10]),
        ('Robert', [15, 13, 55], [5, None, 29]),
        ('James', [11, 13, 45], [5, 89, 79]),
    ],
    ['name', 'scores1', 'scores2']
)

orders = spark.read.load(
    "../data/retail/orders",
    format="csv",
    sep=",",
    schema="order_id int, order_date string, customer_id int, order_status string"
)

order_items = spark.read.load(
    "../data/retail/order_items",
    format="csv",
    sep=",",
    schema=["item_id", "order_id", "product_id", "quantity", "subtotal", "product_price"]
)

if False:
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


    print(">> Date Functions")

    df = spark.range(1)

    df.withColumn(
        "current_date", F.current_date()
    ).withColumn(
        "next_day(Thu)", F.next_day(F.col("current_date"),"Thu")
    ).withColumn(
        "last_day", F.last_day(F.col("current_date"))
    ).withColumn(
        "dayofyear", F.dayofyear(F.col("current_date"))
    ).withColumn(
        "weekofyear", F.weekofyear(F.col("current_date"))
    ).withColumn(
        "quarter", F.quarter(F.col("current_date"))
    ).show(truncate=False)



    df.withColumn(
        "current_date", F.current_date()
    ).withColumn(
        "halloween", F.make_date(F.year(F.col("current_date")), F.lit("10"), F.lit("31"))
    ).withColumn(
        "months_to_halloween", F.months_between(F.col("halloween"), F.col("current_date"))
    ).withColumn(
        "date_diff", F.date_diff(F.col("halloween"), F.col("current_date"))
    ).withColumn(
        "date_add(10)", F.date_add(F.col("current_date"),10)
    ).withColumn(
        "date_trunc(year)", F.date_trunc("year",F.col("current_date"))
    ).withColumn(
        "date_format(yyyy/mm)", F.date_format(F.col("current_date"), "yyyy/MM")
    ).show(truncate=False)


    print(">> Null/NaN functions")
    df = spark.createDataFrame(
        [
            ('A', None, 5.6),
            ('B', 114.0, 6.2),
            ('C', 22.0, float('nan')),
            ('D', float('nan'), 42.1),
            ('E', 53.0, None),
        ],
        ("id","val1","val2")
    )

    df.withColumn(
        "isnull(val1)", F.isnull(df.val1)
    ).withColumn(
        "isnan(val1)", F.isnan(df.val1)
    ).withColumn(
        "nanvl", F.nanvl(df.val1,df.val2)
    ).withColumn(
        "coalesce", F.coalesce(df.val1,df.val2)
    ).show(truncate=False)

    print(">> Collection functions")

    df = spark.sql("SELECT array(struct(1,'a'), struct(2,'b')) as data")

    class_data.select(
        F.size("languages"), F.size("properties")
    ).show()

    class_data.select(
        F.element_at("languages",1), F.element_at("properties",'hair')
    ).show()

    class_data.withColumn(
        "last_name", F.lit("Harris")
    ).select(
        F.struct("first_name","last_name"),
    ).show()

    class_data.withColumn(
        "max", F.array_max("languages")
    ).withColumn(
        "min",  F.array_min("languages")
    ).withColumn(
        "distinct",  F.array_distinct("languages")
    ).withColumn(
        "slice",  F.slice("languages", 2,4)
    ).withColumn(
        "sort",  F.array_sort("languages")
    ).show(truncate=False)

    test_scores.withColumn(
        "union",  F.array_union("scores1","scores2")
    ).withColumn(
        "except",  F.array_except("scores1","scores2")
    ).withColumn(
        "intersection",  F.array_intersect("scores1","scores2")
    ).withColumn(
        "join",  F.array_join("scores2",'|', null_replacement="*")
    ).withColumn(
        "zip(merge)",  F.arrays_zip("scores1","scores2")
    ).withColumn(
        "overlap",  F.arrays_overlap("scores1","scores2")
    ).show(truncate=False)


    class_data.withColumn(
        "last_name", F.lit("Harris")
    ).withColumn(
        "create map", F.create_map("first_name","last_name")
    ).show(truncate=False)

    df.select(F.map_from_entries("data")).show(truncate=False)

    test_scores.withColumn(
        "map_from_arrays", F.map_from_arrays("scores1","scores2")
    ).withColumn(
        "keys", F.map_keys("map_from_arrays")
    ).withColumn(
        "values", F.map_values("map_from_arrays")
    ).show(truncate=False)

    test_scores.withColumn(
        "score1", F.col("scores1")[0]
    ).withColumn(
        "score2", F.col("scores2")[0]
    ).withColumn(
        "sequence", F.sequence("score1", "score2")
    ).show(truncate=False)

    print ("\n\n>> Explode")

    print("Original")
    class_data.show(truncate=False)
    print("Explode Array")
    class_data.select(class_data.first_name, class_data.languages, F.explode(class_data.languages)).show(truncate=False)
    print("Explode Map")
    class_data.select(class_data.first_name, class_data.properties, F.explode(class_data.properties)).show(truncate=False)

    data = [
        (1, [[1],[2,3]]),
        (2, [[5]]),
        (3, None)
    ]
    flatten_df = spark.createDataFrame(data, ['id','data'])
    print("Original")
    flatten_df.show(truncate=False)
    print("Flattened")
    flatten_df.select(flatten_df.id, F.flatten(flatten_df.data)).show(truncate=False)

print("\n\n>> Formatting functions")
order_items