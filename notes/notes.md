## Intro to Spark

- decouples processing and storage
    - spark does all processing in RAM before writing output to disk (better performance than prev solutions)
- agnostic to cluster managers
    - outs is kubernetes
- spark core: basic operations
- spark SQL / Dataframes: batch processing
- spark streaming: stream data processing
- MLib: machine learning
- GraphiX: graph computations


## SparkSession

- entry point for working with RDD, Dataframes, etc
- `spark-submit` for submitting spark jobs to a cluster or executing locally
- lots of random info about options that might be useful to reference later but feels irrelevant now

## Working with RDD
 - low level API all DF and such are built on top of it
 - 2 structure types: RDD & Dataframes
 - Resilient Distributed Dataframes
   - immutable once created
   - fault tolerant
   - distributed into smaller chunks called Partitions across multiple nodes in the cluster
 - lazy evaluation
   - transformations - lazy
   - actions - immediately materialized
 - immutable / in-memory computation
   - all transformations happen in memory
   - data is not stored unless you do it manually
 - structured or semi-structured data?
 - not the best for optimizing performance
- [RDD programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
### shuffling & combining
- **shuffling**
  - process of redistributing data across partitions or nodes
  - creates new stage & very expensive I/O ==> avoid as much as possible!
  - transforms that use shuffle:
    - `groupByKey`, `reduceByKey`, `join`, `union`, `cogroup`, `groupBy`, `distinct`, etc
  - transforms that do **not** use shuffle:
    - `count`, `countByKey`
- **combiner**
  - computes intermediate values for each partition to avoid partial shuffling
  - if you can't avoid shuffling - use a combiner
  - `reduceByKey`, `aggregateByKey`, `combineByKey` both use combiners (preferred)
    - `groupByKey` doesn't!

### RDD Actions
- `take` gets a certain number of elements, seems ordered (ie always takes the first n)
- `collect` collect all elements into a list

#### total aggregations
- `count` counts number of rows
- `reduce` reduce using a commutative/associative binary operator


### RDD Transformations

#### row-level transforms
- `map` applies function to all rows
- `flatMap` same as map, but then flattens results
- `filter` it.... filters
- `mapValues` maps a function to the values of each key in the rdd (inputs/returns key-value pairs)

#### joins
- `join` intersection only
  - `leftOuterJoin` intersection + left 
  - `rightOuterJoin` intersection + right
  - `fullOuterJoin` everything from both left and right
- `cogroup` outer joins on keys and returns in format (key, (iterator_left, iterator_right))
- `cartesian` cross join

#### key aggregations
- `groupByKey` group all data by key
  - no combiner!! use as last resort
  - input: (K, V)
  - output: (K, Iterable<V>)
- `aggregateByKey` aggregate values by key using given agg function
  - agg elements in each partition, then agg results of all partitions
  - input: (K,V)
  - output: any type RDD
  - args
    - zero value: init value for accumulator (0 for int, NULL for collections, etc)
    - seqOp: accumulator function (U,T) => U, where U is the result of each partition
      - arg1: accumulator
      - arg2: next data element
    - combOp: function used to combine all partition results U
- `reduceByKey` aggregate values by key using given reduce function
  - input: (K,V)
  - output: (K,V)
  - associative reduction?
- `countByKey` count values by key
  - input: (K,V)
  - output: Collection Dictionary - (K,int)

### sorting
- `sortByKey` sorts... by key
  - input: (K,V)
  - output: (K,V)