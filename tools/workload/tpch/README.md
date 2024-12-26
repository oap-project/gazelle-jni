# Test on Velox backend with TPC-H workload

## Test datasets
Parquet and DWRF(a fork of the ORC file format) format files are both supported. Here are the steps to generate the testing datasets:

### Generate the Parquet dataset
Please refer to the scripts in [parquet_dataset](./gen_data/parquet_dataset/) directory to generate parquet dataset. Note this script relies on the [spark-sql-perf](https://github.com/databricks/spark-sql-perf) and [tpch-dbgen](https://github.com/databricks/tpch-dbgen) package from Databricks. Note in the tpch-dbgen kits, we need to do a slight modification to allow Spark to convert the csv based content to parquet, please make sure to use this commit: [0469309147b42abac8857fa61b4cf69a6d3128a8](https://github.com/databricks/tpch-dbgen/commit/0469309147b42abac8857fa61b4cf69a6d3128a8)


In tpch_datagen_parquet.sh, several parameters should be configured according to the system.
```
spark_sql_perf_jar=/PATH/TO/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
...
  --num-executors 14 
  --executor-cores 8 
  --conf spark.sql.shuffle.partitions=224 
...
```

In tpch_datagen_parquet.scala, the parameters and dirs should be configured as well.
```
val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
val numPartitions = 200  // how many dsdgen partitions to run - number of input tasks.
...
val rootDir = "/PATH/TO/TPCH_PARQUET_PATH" // root directory of location to create data in.
val dbgenDir = "/PATH/TO/TPCH_DBGEN" // location of dbgen
```

Currently, Gluten with Velox can support both Parquet and DWRF file format and three compression codec including snappy, gzip, zstd.
Below step, to convert Parquet to DWRF, is optional if you are using Parquet format to run the testing.

### Convert the Parquet dataset to DWRF dataset(OPTIONAL)
And then please refer to the scripts in [dwrf_dataset](./gen_data/dwrf_dataset/) directory to convert the Parquet dataset to DWRF dataset.

In tpch_convert_parquet_dwrf.sh, spark configures should be set according to the system.

```
export GLUTEN_HOME=/PATH/TO/gluten
...
--executor-cores 8                                      \
--num-executors 14                                       \
```

In tpch_convert_parquet_dwrf.scala, the table path should be configured.
```
val parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
val dwrf_file_path = "/PATH/TO/TPCH_DWRF_PATH"
```

## Test Queries
We provide the test queries in [TPC-H queries](../../../tools/gluten-it/common/src/main/resources/tpch-queries).
We also provide a scala script in [Run TPC-H](./run_tpch/) directory about how to run TPC-H queries.
Please note if you are using DWRF test, please remember to set the file format to DWRF in the code.
