# Shuffle
## What is
- Redistributing data across partitions
- Copy data across executors

## Problem 
- bad performance
- High disk I/O
- High network I/O
- data serialization

## Operation
- repartition
- coalesce
- groupByKey
- reduceByKey
- join

## Solution
- spark.shuffle.compress
- spark.shuffle.file.buffer
- spark.shuffle.service.enable
- Broadcast join if one dataset is small
- SortMerge join if both datasets are large
- Avoid count and repartition in production
- Use coalesce instead of repartition
- Use approxCountDistinct instead of Distinctcount
- Use dropDuplicates before join and groupBy

# Minimize Scan
## What is
- Read less to read faster

## Problem
- read is slow

## Operation
- read

## Solution
- Partition
- Bucketing
- partition pruning
- Filtering
- Avoid data skewness

# Caching
## What is
- Persist dataframe for reusing in subsequent action

## Problem
- Garbace Colector overhead
- Disk spills
- Possible slowdown

# Operation
- cache
- checkpoint

## Solution
- verify logs in Spark UI to identify if there is a problem
- only use when is necessery

# Foreach
- Use seq.par.foreach instead

# Parquet
- best file format for Spark
- better with snappy compression

# Parallelism
- Increase performance
- spark.deafult.árallelism
- spark.sql.files.minPartitionNum

# UDF
- To increase performance (up to 10x) use config.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

# Dynamic allocation
- spark.dynamicAllocation.enabled
- spark.dynamicAllocation.minExecutors
- spark.dynamicAllocation.maxExecutors
- spark.dynamicAllocation.executorIdleTimeout