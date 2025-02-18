---
layout: page
title: Gluten with Velox Backend
nav_order: 1
parent: Getting-Started
---

# Supported Version

| Type  | Version                      |
|-------|------------------------------|
| Spark | 3.2.2, 3.3.1, 3.4.4, 3.5.4   |
| OS    | Ubuntu20.04/22.04, Centos7/8 |
| jdk   | openjdk8/jdk17               |
| scala | 2.12                         |

# Prerequisite

Currently, with static build Gluten+Velox backend supports all the Linux OSes, but is only tested on **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8**. With dynamic build, Gluten+Velox backend support **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8** and their variants.

Currently, the officially supported Spark versions are 3.2.2, 3.3.1, 3.4.4 and 3.5.4.

We need to set up the `JAVA_HOME` env. Currently, Gluten supports **java 8** and **java 17**.

**For x86_64**

```bash
## make sure jdk8 is used
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**For aarch64**

```bash
## make sure jdk8 is used
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
export PATH=$JAVA_HOME/bin:$PATH
```

**Get gluten**

```bash
## config maven, like proxy in ~/.m2/settings.xml

## fetch gluten code
git clone https://github.com/apache/incubator-gluten.git
```

# Build Gluten with Velox Backend

It's recommended to use buildbundle-veloxbe.sh to build gluten in one script.
[Gluten build guide](./build-guide.md) listed the parameters and their default value of build command for your reference.

**For x86_64 build**

First time build for all supported spark versions.

```bash
./dev/buildbundle-veloxbe.sh
```

After a complete build, if only some gluten code is changed, you can use the following command to skip building arrow and
setting up build dependencies.

```bash
./dev/buildbundle-veloxbe.sh --build_arrow=OFF --run_setup_script=OFF
```

**For aarch64 build**

```bash
export CPU_TARGET="aarch64"

./dev/buildbundle-veloxbe.sh
```

**Step-by-step build**

Alternative to the above one-step build, you can follow the below guide for step-by-step build.
Currently, Gluten is using a [forked Velox](https://github.com/oap-project/velox/) which is daily updated based on [upstream Velox](https://github.com/facebookincubator/velox).

```bash

# Build arrow with some patches applied. We need these slight code changes till arrow is upgraded
# to 17.0.0 or newer versions.
./dev/builddeps-veloxbe.sh build_arrow

./dev/builddeps-veloxbe.sh build_velox

./dev/builddeps-veloxbe.sh build_gluten_cpp

## compile Gluten java module and create package jar
cd /path/to/gluten
# For spark3.2.x
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.2 -DskipTests
# For spark3.3.x
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.3 -DskipTests
# For spark3.4.x
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.4 -DskipTests
# For spark3.5.x
mvn clean package -Pbackends-velox -Pceleborn -Puniffle -Pspark-3.5 -DskipTests
```

Notes： Building Velox may fail caused by OOM. You can prevent this failure by adjusting `NUM_THREADS` (e.g., `export NUM_THREADS=4`) before building Gluten/Velox. The recommended minimal memory size is 64G.

After the above build process, the Jar file will be generated under `package/target/`.

Alternatively you may refer to [build in docker](docs/developers/velox-backend-build-in-docker.md) to build the gluten jar in docker.

## Dependency library deployment

With build option `enable_vcpkg=ON`, all dependency libraries will be statically linked to `libvelox.so` and `libgluten.so` which are packed into the gluten-jar.
In this way, only the gluten-jar is needed to add to `spark.<driver|executor>.extraClassPath` and spark will deploy the jar to each worker node. It's better to build
the static version using a clean docker image without any extra libraries installed ( [build in docker](docs/developers/velox-backend-build-in-docker.md) ). On host with
some libraries like jemalloc installed, the script may crash with odd message. You may need to uninstall those libraries to get a clean host. We **strongly recommend** user to build Gluten in this way to avoid dependency lacking issue.

With build option `enable_vcpkg=OFF`, not all dependency libraries will be dynamically linked. After building, you need to separately execute `./dev/build-thirdparty.sh` to 
pack required shared libraries into another jar named `gluten-thirdparty-lib-$LINUX_OS-$VERSION-$ARCH.jar`. Then you need to add the jar to Spark config `extraClassPath` and 
set `spark.gluten.loadLibFromJar=true`. Otherwise, you need to install required shared libraries with **exactly the same versions** on each worker node . You may find the 
libraries list from the third-party jar.

# Remote storage support

## HDFS support

Gluten supports dynamically loading both libhdfs.so and libhdfs3.so at runtime by using dlopen, allowing the JVM to load the appropriate shared library file as needed. This means you do not need to set the library path during the compilation phase.
To enable this functionality, you must set the JAVA_HOME and HADOOP_HOME environment variables. Gluten will then locate and load the ${HADOOP_HOME}/lib/native/libhdfs.so file at runtime. If you prefer to use libhdfs3.so instead, simply replace the ${HADOOP_HOME}/lib/native/libhdfs.so file with libhdfs3.so.

### Build libhdfs3

If you want to run Gluten with libhdfs3.so, you need to manually compile libhdfs3 to obtain the libhdfs3.so file. We provide the script dev/build_libhdfs3.sh in Gluten to help you compile libhdfs3.so.

### Build with HDFS support

To build Gluten with HDFS support, below command is suggested:

```bash
cd /path/to/gluten
./dev/buildbundle-veloxbe.sh --enable_hdfs=ON
```

### Configuration about HDFS support in Libhdfs3

HDFS uris (hdfs://host:port) will be extracted from a valid hdfs file path to initialize hdfs client, you do not need to specify it explicitly.

libhdfs3 need a configuration file and [example here](https://github.com/apache/hawq/blob/e9d43144f7e947e071bba48871af9da354d177d0/src/backend/utils/misc/etc/hdfs-client.xml), this file is a bit different from hdfs-site.xml and core-site.xml.
Download that example config file to local and do some needed modifications to support HA or else, then set env variable like below to use it, or upload it to HDFS to use, more details [here](https://github.com/apache/hawq/blob/e9d43144f7e947e071bba48871af9da354d177d0/depends/libhdfs3/src/client/Hdfs.cpp#L171-L189).

```
// Spark local mode
export LIBHDFS3_CONF="/path/to/hdfs-client.xml"

// Spark Yarn cluster mode
--conf spark.executorEnv.LIBHDFS3_CONF="/path/to/hdfs-client.xml"

// Spark Yarn cluster mode and upload hdfs config file
cp /path/to/hdfs-client.xml hdfs-client.xml
--files hdfs-client.xml
```

One typical deployment on Spark/HDFS cluster is to enable [short-circuit reading](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html). Short-circuit reads provide a substantial performance boost to many applications.

By default libhdfs3 does not set the default hdfs domain socket path to support HDFS short-circuit read. If this feature is required in HDFS setup, users may need to setup the domain socket path correctly by patching the libhdfs3 source code or by setting the correct config environment. In Gluten the short-circuit domain socket path is set to "/var/lib/hadoop-hdfs/dn_socket" in [build_velox.sh](https://github.com/apache/incubator-gluten/blob/main/ep/build-velox/src/build_velox.sh) So we need to make sure the folder existed and user has write access as below script.

```
sudo mkdir -p /var/lib/hadoop-hdfs/
sudo chown <sparkuser>:<sparkuser> /var/lib/hadoop-hdfs/
```

You also need to add configuration to the "hdfs-site.xml" as below:

```
<property>
  <name>dfs.client.read.shortcircuit</name>
  <value>true</value>
</property>
<property>
  <name>dfs.domain.socket.path</name>
  <value>/var/lib/hadoop-hdfs/dn_socket</value>
</property>
```

### Kerberos support in libhdfs3

Here are two steps to enable kerberos.

- Make sure the hdfs-client.xml contains

```
<property>
  <name>hadoop.security.authentication</name>
  <value>kerberos</value>
</property>
```

- Specify the environment variable [KRB5CCNAME](https://github.com/apache/hawq/blob/e9d43144f7e947e071bba48871af9da354d177d0/depends/libhdfs3/src/client/FileSystem.cpp#L56) and upload the kerberos ticket cache file

```
--conf spark.executorEnv.KRB5CCNAME=krb5cc_0000  --files /tmp/krb5cc_0000
```

The ticket cache file can be found by `klist`.

## Azure Blob File System (ABFS) support

Velox supports ABFS with the open source [Azure SDK for C++](https://github.com/Azure/azure-sdk-for-cpp) and Gluten uses the Velox ABFS connector to connect with ABFS.
The build option for ABFS (enable_abfs) must be set to enable this feature as listed below.

```
cd /path/to/gluten
./dev/buildbundle-veloxbe.sh --enable_abfs=ON
```

Please refer [Velox ABFS](VeloxABFS.md) part for more detailed configurations.


## AWS S3 support

Velox supports S3 with the open source [AWS C++ SDK](https://github.com/aws/aws-sdk-cpp) and Gluten uses Velox S3 connector to connect with S3.
A new build option for S3(enable_s3) is added. Below command is used to enable this feature

```
cd /path/to/gluten
./dev/buildbundle-veloxbe.sh --enable_s3=ON
```

Currently there are several ways to access S3 in Spark. Please refer [Velox S3](VeloxS3.md) part for more detailed configurations

## GCS support

Please refer [GCS](VeloxGCS.md)

# Remote Shuffle Service Support

## Celeborn support

Gluten with velox backend supports [Celeborn](https://github.com/apache/celeborn) as remote shuffle service. Currently, the supported Celeborn versions are `0.3.x`, `0.4.x` and `0.5.x`.

Below introduction is used to enable this feature.

First refer to this URL(https://github.com/apache/celeborn) to setup a celeborn cluster.

When compiling the Gluten Java module, it's required to enable `celeborn` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Pceleborn -DskipTests
```

Then add the Gluten and Spark Celeborn Client packages to your Spark application's classpath(usually add them into `$SPARK_HOME/jars`).

- Celeborn: celeborn-client-spark-3-shaded_2.12-[celebornVersion].jar
- Gluten: gluten-velox-bundle-spark3.x_2.12-xx_xx_xx-SNAPSHOT.jar (The bundled Gluten Jar. Make sure -Pceleborn is specified when it is built.)

Currently to use Gluten following configurations are required in `spark-defaults.conf`

```
spark.shuffle.manager org.apache.spark.shuffle.gluten.celeborn.CelebornShuffleManager

# celeborn master
spark.celeborn.master.endpoints clb-master:9097

spark.shuffle.service.enabled false

# options: hash, sort
# Hash shuffle writer use (partition count) * (celeborn.push.buffer.max.size) * (spark.executor.cores) memory.
# Sort shuffle writer uses less memory than hash shuffle writer, if your shuffle partition count is large, try to use sort hash writer.  
spark.celeborn.client.spark.shuffle.writer hash

# We recommend setting spark.celeborn.client.push.replicate.enabled to true to enable server-side data replication
# If you have only one worker, this setting must be false 
# If your Celeborn is using HDFS, it's recommended to set this setting to false
spark.celeborn.client.push.replicate.enabled true

# Support for Spark AQE only tested under Spark 3
# we recommend setting localShuffleReader to false to get better performance of Celeborn
spark.sql.adaptive.localShuffleReader.enabled false

# If Celeborn is using HDFS
spark.celeborn.storage.hdfs.dir hdfs://<namenode>/celeborn

# If you want to use dynamic resource allocation,
# please refer to this URL (https://github.com/apache/celeborn/tree/main/assets/spark-patch) to apply the patch into your own Spark.
spark.dynamicAllocation.enabled false
```

## Uniffle support

Uniffle with velox backend supports [Uniffle](https://github.com/apache/incubator-uniffle) as remote shuffle service. Currently, the supported Uniffle versions are `0.9.2`.

First refer to this URL(https://uniffle.apache.org/docs/intro) to get start with uniffle.

When compiling the Gluten Java module, it's required to enable `uniffle` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Puniffle -DskipTests
```

Then add the Uniffle and Spark Celeborn Client packages to your Spark application's classpath(usually add them into `$SPARK_HOME/jars`).

- Uniffle: rss-client-spark3-shaded-[uniffleVersion].jar
- Gluten: gluten-velox-bundle-spark3.x_2.12-xx_xx_xx-SNAPSHOT.jar (The bundled Gluten Jar. Make sure -Puniffle is specified when it is built.)

Currently to use Gluten following configurations are required in `spark-defaults.conf`

```
spark.shuffle.manager org.apache.spark.shuffle.gluten.uniffle.UniffleShuffleManager

# uniffle coordinator address
spark.rss.coordinator.quorum ip:port

# Support for Spark AQE
spark.sql.adaptive.localShuffleReader.enabled false
spark.shuffle.service.enabled false

# Uniffle support mutilple storage types, you can choose one of them.
# Such as MEMORY,LOCALFILE,MEMORY_LOCALFILE,HDFS,MEMORY_HDFS,LOCALFILE_HDFS,MEMORY_LOCALFILE_HDFS
spark.rss.storage.type LOCALFILE_HDFS

# If you want to use dynamic resource allocation,
# please refer to this URL (https://uniffle.apache.org/docs/client-guide#support-spark-dynamic-allocation) for more details.
spark.dynamicAllocation.enabled false
```

# Datalake Framework Support

## DeltaLake Support

Gluten with velox backend supports [DeltaLake](https://delta.io/) table.

### How to use

First of all, compile gluten-delta module by a `delta` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Pdelta -DskipTests
```

Once built successfully, delta features will be included in gluten-velox-bundle-X jar. Then you can query delta table by gluten/velox without scan's fallback.

Gluten with velox backends also support the column mapping of delta tables.
About column mapping, see more [here](https://docs.delta.io/latest/delta-column-mapping.html).

## Iceberg Support

Gluten with velox backend supports [Iceberg](https://iceberg.apache.org/) table. Currently, both reading COW (Copy-On-Write) and MOR (Merge-On-Read) tables are supported.

### How to use

First of all, compile gluten-iceberg module by a `iceberg` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Piceberg -DskipTests
```

Once built successfully, iceberg features will be included in gluten-velox-bundle-X jar. Then you can query iceberg table by gluten/velox without scan's fallback.

## Hudi Support

Gluten with velox backend supports [Hudi](https://hudi.apache.org/) table. Currently, only reading COW (Copy-On-Write) tables is supported.

### How to use

First of all, compile gluten-hudi module by a `hudi` profile, as follows:

```
mvn clean package -Pbackends-velox -Pspark-3.3 -Phudi -DskipTests
```

Once built successfully, hudi features will be included in gluten-velox-bundle-X jar. Then you can query hudi **COW** table by gluten/velox without scan's fallback.

# Coverage

Spark3.3 has 387 functions in total. ~240 are commonly used. To get the support status of all Spark built-in functions, please refer to [Velox Backend's Supported Operators & Functions](../velox-backend-support-progress.md).

> Velox doesn't support [ANSI mode](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)), so as Gluten. Once ANSI mode is enabled in Spark config, Gluten will fallback to Vanilla Spark.

To identify what can be offloaded in a query and detailed fallback reasons, user can follow below steps to retrieve corresponding logs.

```
1) Enable Gluten by proper [configuration](https://github.com/apache/incubator-gluten/blob/main/docs/Configuration.md).

2) Disable Spark AQE to trigger plan validation in Gluten
spark.sql.adaptive.enabled = false

3) Check physical plan 
sparkSession.sql("your_sql").explain()
```

With above steps, you will get a physical plan output like:

```
== Physical Plan ==
-Execute InsertIntoHiveTable (7)
  +- Coalesce (6)
    +- VeloxColumnarToRowExec (5)
      +- ^ ProjectExecTransformer (3)
        +- GlutenRowToArrowColumnar (2)
          +- Scan hive default.extracted_db_pins (1)

```

`GlutenRowToArrowColumnar`/`VeloxColumnarToRowExec` indicates there is a fallback operator before or after it. And you may find fallback reason like below in logs.

```
native validation failed due to: in ProjectRel, Scalar function name not registered: get_struct_field, called with arguments: (ROW<col_0:INTEGER,col_1:BIGINT,col_2:BIGINT>, INTEGER).
```

In the above, the symbol `^` indicates a plan is offloaded to Velox in a stage. In Spark DAG, all such pipelined plans (consecutive plans marked with `^`) are plotted
inside an umbrella node named `WholeStageCodegenTransformer` (It's not codegen node. The naming is just for making it well plotted like Spark Whole Stage Codegen).

# Spill

Velox backend supports spilling-to-disk.

Using the following configuration options to customize spilling:

| Name                                                                     | Default Value | Description                                                                                                                                                                       |
|--------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.gluten.sql.columnar.backend.velox.spillStrategy                    | auto          | none: Disable spill on Velox backend; auto: Let Spark memory manager manage Velox's spilling                                                                                      |
| spark.gluten.sql.columnar.backend.velox.spillFileSystem                  | local         | The filesystem used to store spill data. local: The local file system. heap-over-local: Write files to JVM heap if having extra heap space. Otherwise write to local file system. |
| spark.gluten.sql.columnar.backend.velox.aggregationSpillEnabled          | true          | Whether spill is enabled on aggregations                                                                                                                                          |
| spark.gluten.sql.columnar.backend.velox.joinSpillEnabled                 | true          | Whether spill is enabled on joins                                                                                                                                                 |
| spark.gluten.sql.columnar.backend.velox.orderBySpillEnabled              | true          | Whether spill is enabled on sorts                                                                                                                                                 |
| spark.gluten.sql.columnar.backend.velox.maxSpillLevel                    | 4             | The max allowed spilling level with zero being the initial spilling level                                                                                                         |
| spark.gluten.sql.columnar.backend.velox.maxSpillFileSize                 | 1GB           | The max allowed spill file size. If it is zero, then there is no limit                                                                                                            |
| spark.gluten.sql.columnar.backend.velox.spillStartPartitionBit           | 29            | The start partition bit which is used with 'spillPartitionBits' together to calculate the spilling partition number                                                               |
| spark.gluten.sql.columnar.backend.velox.spillPartitionBits               | 2             | The number of bits used to calculate the spilling partition number. The number of spilling partitions will be power of two                                                        |
| spark.gluten.sql.columnar.backend.velox.spillableReservationGrowthPct    | 25            | The spillable memory reservation growth percentage of the previous memory reservation size                                                                                        |
| spark.gluten.sql.columnar.backend.velox.spillThreadNum                   | 0             | (Experimental) The thread num of a dedicated thread pool to do spill

# Velox User-Defined Functions (UDF) and User-Defined Aggregate Functions (UDAF)

Please check the [VeloxNativeUDF.md](../developers/VeloxNativeUDF.md) for more detailed usage and configurations.

# Test TPC-H or TPC-DS on Gluten with Velox backend

All TPC-H and TPC-DS queries are supported in Gluten Velox backend. You may refer to the [notebook](../../tools/workload/benchmark_velox) we used to do the performance test.

## Data preparation

The data generation scripts are [TPC-H dategen script](../../tools/workload/tpch/gen_data/parquet_dataset/tpch_datagen_parquet.sh) and
[TPC-DS dategen script](../../tools/workload/tpcds/gen_data/parquet_dataset/tpcds_datagen_parquet.sh).

The used TPC-H and TPC-DS queries are the original ones, and can be accessed from [TPC-DS queries](../../tools/gluten-it/common/src/main/resources/tpcds-queries)
and [TPC-H queries](../../tools/gluten-it/common/src/main/resources/tpch-queries).

Some other versions of TPC-DS queries are also provided, but are **not** recommended for testing, including:

- the modified TPC-DS queries with "Decimal-to-Double": [TPC-DS non-decimal queries](../../gluten-core/src/test/resources/tpcds-queries/tpcds.queries.no-decimal) (outdated).

## Submit the Spark SQL job

Submit test script from spark-shell. You can find the scala code to [Run TPC-H](../../tools/workload/tpch/run_tpch/tpch_parquet.scala) as an example. Please remember to modify
the location of TPC-H files as well as TPC-H queries before you run the testing.

```
var parquet_file_path = "/PATH/TO/TPCH_PARQUET_PATH"
var gluten_root = "/PATH/TO/GLUTEN"
```

Below script shows an example about how to run the testing, you should modify the parameters such as executor cores, memory, offHeap size based on your environment.

```bash
export GLUTEN_JAR = /PATH/TO/GLUTEN/package/target/<gluten-jar>
cat tpch_parquet.scala | spark-shell --name tpch_powertest_velox \
  --master yarn --deploy-mode client \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.driver.extraClassPath=${GLUTEN_JAR} \
  --conf spark.executor.extraClassPath=${GLUTEN_JAR} \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=20g \
  --conf spark.gluten.sql.columnar.forceShuffledHashJoin=true \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --num-executors 6 \
  --executor-cores 6 \
  --driver-memory 20g \
  --executor-memory 25g \
  --conf spark.executor.memoryOverhead=5g \
  --conf spark.driver.maxResultSize=32g
```

Refer to [Gluten configuration](../Configuration.md) for more details.

## Result

*wholestagetransformer* indicates that the offloading works.

![TPC-H Q6](../image/TPC-H_Q6_DAG.png)

## Performance

Below table shows the TPC-H Q1 and Q6 Performance in a multiple-thread test (--num-executors 6 --executor-cores 6) for Velox and vanilla Spark.
Both Parquet and ORC datasets are sf1024.

| Query Performance (s) | Velox (ORC) | Vanilla Spark (Parquet) | Vanilla Spark (ORC) |
|---------------- | ----------- | ------------- | ------------- |
| TPC-H Q6 | 13.6 | 21.6  | 34.9 |
| TPC-H Q1 | 26.1 | 76.7 | 84.9 |

# Gluten UI

Please refer [Gluten UI](VeloxGlutenUI.md)

# Gluten Implicits

Gluten provides a helper class to get the fallback summary from a Spark Dataset.

```
import org.apache.spark.sql.execution.GlutenImplicits._
val df = spark.sql("SELECT * FROM t")
df.fallbackSummary
```

Note that, if AQE is enabled, but the query is not materialized, then it will re-plan
the query execution with disabled AQE. It is a workaround to get the final plan, and it may
cause the inconsistent results with a materialized query. However, we have no choice.

# Accelerators

Please refer [HBM](VeloxHBM.md) [QAT](VeloxQAT.md) [IAA](VeloxIAA.md) for details
