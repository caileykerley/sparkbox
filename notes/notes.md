## Intro to Spark

- decouples processing and storage
    - spark does all processing in RAM before writing output to disk (better performance than prev solutions)
- agnostic to cluster managers
    - ours is kubernetes
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
  - `takeOrdered` get n elements from RDD ordered asc/dec or by optional key
  - `takeSample` get n random elements from RDD
    - withReplacement: True/False
    - num: int number of records
    - seed: optional float
- `collect` collect all elements into a list

#### total aggregations
- `count` counts number of rows
- `reduce` reduce using a commutative/associative binary operator


### RDD Transformations

#### row-level transforms
- `map` applies function to all rows
- `flatMap` same as map, but then flattens results (returns a list?)
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

#### sorting
- `sortByKey` sorts... by key
  - input: (K,V)
  - output: (K,V)

#### ranking
- no direct way to do this with RDD, but you can accomplish it by combining transforms
  - sortByKey + take
  - takeOrdered or top
- rank within groups
  - groupByKey + flatmap?

#### set ops
- requires similarly structured RDDs
- `union` get all elements from both RDDs
  - does NOT return distinct elements
- `intersection` get common elements from both RDDs
  - returns distinct elements
- `subtract` get values in left RDD that are not in right RDD

#### sampling
- `sample` get a random sample of records
  - withReplacement: True/False
  - fraction: float (0,1)
  - seed: optional float


### Partitions and Coalesces
- datasets are big, so partitioned across multiple machines
- one partion may NOT span more than 1 machine
- spark automatically partitions, but you can also configure optimal num of partitions
- `getNumPartitions` tells you # partitions
- `glom` returns RDD created by coalescing all elements within each partition into a list
- `coalesce`: reduce number of partitions
  - good for cases where a filter reduces dataset size
  - does not guarantee equal sized partitions unless shuffle=True
  - shuffling is optional, so can be more efficient than repartition
  - can be used for increasing num partitions only when shuffle=True
- `repartition`: shuffles and returns rdd with exact number of partitions requested
  - spark is more efficient when partitions are around the same size
  - good to use to ensure equal sized partitions
- `repartitionAndSortWithinPartitions`
  - repartition according to a given partition function and sort within each resulting partition by record key
- it's good to repartition/coalesce after joining/filtering to make partitions more efficient


## Execution Architecture
- application: user spark application
- job: parallel computation made of multiple tasks triggered by spark action
- stage: sequential steps in job execution, each stage is made of up tasks and depends on previous stage completion
- task: unit of work that will be sent to one executor
- application jar: jar containing spark application code
- driver program: the process running main() and creating spark context
- cluster manager: external service for acquiring resources on cluster (spark is agnostic to cluster manager)
- deploy mode: distinguishes where driver process runs; "cluster" - launches inside cluster, "client" - launches outside cluster
- worker node: any cluster node that can run tasks
- executor: process on worker node that runs task and manages data
- cache: job resources needed by workers for task execution

### Narrow vs Wide Transforms
- *narrow* transforms convert each input partition into a single output partition
  - spark merges all narrow transforms in one stage of execution 
    - eg map -> filter -> map, will all get executed at the same time - very efficient
  - examples: map, filter
  - fast, no shuffling
- *wide* input partitions may contribute to many output partitions
  - each wide transform creates new stage
  - slower, shuffling
  - examples: groupByKey, aggregateByKey, join, distinct, repartition 

### DAG Scheduler
- directed acyclic graph
- used to solve for order of task execution
- after each code line, spark finds logical execution plan, but if no action, it stops here
  - made of logical relationship between input and intermediate rdds
- once an action is encountered, spark translates that to the physical execution plan
  - made of actual tasks and stages
- then, all tasks bundled and sent to task scheduler
- RDD lineage
  - each rdd maintains a pointer to one or more parent along with metadata about its relationship to the parent
  - used for recovery if something fails
  - can be printed with toDebugString (prints logical execution plan)
- stages that run independently may be run in parallel

## RDD Persistance 
- `StorageLevel` api for setting storage level
  - disk, memory, off-heap (memory outside the JVM)
  - serialization
  - use replicated storage level for fast fault recovery
- `persist` set RDD storage level to persist its values after the first time its computed
  - storage level may only be set if RDD does not already have a storage level
  - default: memory only
- `is_cached` check whether/not RDD will persist
- `unpersist` un-persist an RDD
- if data can fit in memory, leave it there (most CPU efficient)

## Shared Variables
- variables that need to be used in parallel operations
- broadcast variables
  - var that is read-only and cached on each machine
  - immutable, cached on each worker only once
  - efficient, needs to fit in memory
  - closure - serialized information about variables or methods sent to each executor
- accumulator variables
  - shared var used for sum and counter operations
  - shared by all executors to update using associative/commutative operations
  - best to only use these inside actions


## Spark SQL Architecture
- catalyst: builds & optimizes query program
- tungsten: executes query program
- trees: abstractions of user programs
- expression: represents a new value that a user is calculating
- logical plan: describes computation without defining how to conduct the computation
- physical plan: describes computation with specific definitions for how to conduct the computation
- transforms: rules applied to trees to simplify/optimize queries
  - constant collapse, column pruning, etc
- `explain` prints the logical & physical execution plans for debugging


## SparkSession
- As of Spark 2.0, SparkSession is the main entry point
- `spark-submit` cli utility to run a pyspark app
- default spark properties: `$SPARK_HOME/conf/spark-defaults.conf`

### Common Functions
- `version` spark version
- `range` like native python range, but creates a spark DF instead of a list