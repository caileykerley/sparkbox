from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.master('local').appName('Test').getOrCreate()

orders = spark.sparkContext.textFile('data/retail/orders')
ORDER_ID_ORDERS = 0
ORDER_DATE = 1
CUSTOMER_ID = 2
ORDER_STATUS = 3

order_items = spark.sparkContext.textFile('data/retail/order_items')
ITEM_ID = 0
ORDER_ID_ITEMS = 1
PRODUCT_ID = 2
QUANTITY = 3
SUBTOTAL = 4
PRODUCT_PRICE = 5


# pull out order number as the key
def csv_2_kv(row, ix, vix=None):
    parts = row.split(',')
    if vix is None:
        return parts[ix], parts
    else:
        return parts[ix], parts[vix]


orders_kv = orders.map(lambda row: csv_2_kv(row, ORDER_ID_ORDERS))
order_items_kv = order_items.map(lambda row: csv_2_kv(row, ORDER_ID_ITEMS))

"""
# joins
j = orders_kv.join(order_items_kv)
print(f"inner: {j.count()}")
print(f"outer: {orders_kv.fullOuterJoin(order_items_kv).count()}")
print("(Order ID, (Customer ID, Subtotal))")
print(j.map(lambda row: (row[0], row[1][0][1], row[1][1][3])).first())
print(j.filter(lambda x: x[0] == '4').map(lambda row: (row[0], row[1][0][1], row[1][1][3])).collect())


# co-group
print(orders_kv.cogroup(order_items_kv).take(5))


# cartesian
rdd = spark.sparkContext.parallelize(([1,2,3]))
print(rdd.cartesian(rdd).collect())


# count
print(f"Closed orders: {orders_kv.filter(lambda x: x[1][ORDER_STATUS] == 'CLOSED').count()}")


# reduce
items_sold = order_items_kv.filter(lambda x: int(x[0]) <= 10).map(lambda x: int(x[1][QUANTITY])).reduce(add)
print(f"# items sold in orders 1-10: {items_sold}")

max_subtotal = order_items_kv.filter(lambda x: int(x[0]) == 10).map(lambda x: float(x[1][SUBTOTAL])).reduce(max)
print(f"order 10 max subtotal: {max_subtotal}")


# debug strings
action = order_items_kv.distinct()
print(action.toDebugString())


# groupByKey
print(">>> total revenue by product type (groupByKey)")
print(order_items.map(lambda x: (x.split(',')[PRODUCT_ID], float(x.split(',')[SUBTOTAL]))).groupByKey().mapValues(sum).take(5))


# reduceByKey
print(">>> total revenue by product type (reduceByKey)")
print(order_items.map(lambda x: (x.split(',')[PRODUCT_ID], float(x.split(',')[SUBTOTAL]))).reduceByKey(add).take(5))


# aggregateByKey
print(">>> max revenue for each order")
order_items_kv3 = order_items.map(lambda x: (x.split(',')[ORDER_ID_ITEMS], float(x.split(',')[SUBTOTAL])))
print(order_items_kv3.aggregateByKey(0, max, max).take(5))

print(">>> max revenue for each order with item ID")
max_tuple = lambda acc, data: acc if acc[1] > data[1] else data
order_items_kv3 = order_items.map(lambda x: (x.split(',')[ORDER_ID_ITEMS], (x.split(',')[ITEM_ID], float(x.split(',')[SUBTOTAL]))))
print(order_items_kv3.aggregateByKey(('',0), max_tuple, max_tuple).take(5))

print('>>> sum up all revenue and number of records for each order')
sum_parts = lambda acc, data: acc[0] + float(data[SUBTOTAL]), acc[1] + 1
sum_tuple = lambda acc1, acc2: acc1[0] + acc2[0], acc1[1] + acc2[1]
print(order_items_kv.aggregateByKey((0.0,0), sum_parts, sum_tuple).take(5))


# countByKey
print('>>> number of orders per each status')
status_counts = orders.map(lambda row: csv_2_kv(row, ORDER_STATUS)).countByKey()
for key, value in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{key}: {value}")

"""

# sortByKey
print(">>> sort orders by customer_id")
for vals in orders.map(lambda row: csv_2_kv(row, CUSTOMER_ID)).sortByKey().take(5):
    print(vals[1])

print(">>> sort orders by customer_id and status")
multi_key = orders.map(lambda row: ((row.split(',')[CUSTOMER_ID], row.split(',')[ORDER_STATUS]),row))
for vals in multi_key.sortByKey().take(7):
    print(vals[1])


