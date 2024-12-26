---
layout: page
title: Gluten with Velox Backend
nav_order: 1
parent: Getting-Started
---

# Supported Version

| Type  | Version                      |
|-------|------------------------------|
| Spark | 3.2.2, 3.3.1, 3.4.3, 3.5.1   |
| OS    | Ubuntu20.04/22.04, Centos7/8 |
| jdk   | openjdk8/jdk17               |
| scala | 2.12                         |

# Prerequisite

Currently, Gluten+Velox backend is only tested on **Ubuntu20.04/Ubuntu22.04/Centos7/Centos8**.
Other kinds of OS support are still in progress. The long term goal is to support several common OS and conda env deployment.

Currently, the officially supported Spark versions are 3.2.2, 3.3.1, 3.4.3 and 3.5.1.

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

Notes： Building Velox may fail caused by OOM. You can prevent this failure by adjusting `NUM_THREADS` (e.g., `export NUM_THREADS=4`) before building Gluten/Velox.

After the above build process, the Jar file will be generated under `package/target/`.

## Dependency library deployment

With build option `enable_vcpkg=ON`, all dependency libraries will be statically linked to `libvelox.so` and `libgluten.so` which are packed into the gluten-jar.
In this way, only the gluten-jar is needed to add to `spark.<driver|executor>.extraClassPath` and spark will deploy the jar to each worker node. It's better to build
the static version using a clean docker image without any extra libraries installed. On host with some libraries like jemalloc installed, the script may crash with
odd message. You may need to uninstall those libraries to get a clean host. We strongly recommend user to build Gluten in this way to avoid dependency lacking issue.

With build option `enable_vcpkg=OFF`, not all dependency libraries will be statically linked. You need to separately execute `./dev/build-thirdparty.sh` to pack required
shared libraries into another jar named `gluten-thirdparty-lib-$LINUX_OS-$VERSION-$ARCH.jar`. Then you need to add the jar to Spark config `extraClassPath` and set
`spark.gluten.loadLibFromJar=true`. Otherwise, you need to install required shared libraries on each worker node. You may find the libraries list from the third-party jar.

## HDFS support

Gluten supports dynamically loading both libhdfs.so and libhdfs3.so at runtime by using dlopen, allowing the JVM to load the appropriate shared library file as needed. This means you do not need to set the library path during the compilation phase.
To enable this functionality, you must set the JAVA_HOME and HADOOP_HOME environment variables. Gluten will then locate and load the ${HADOOP_HOME}/lib/native/libhdfs.so file at runtime. If you prefer to use libhdfs3.so instead, simply replace the ${HADOOP_HOME}/lib/native/libhdfs.so file with libhdfs3.so.

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

Uniffle with velox backend supports [Uniffle](https://github.com/apache/incubator-uniffle) as remote shuffle service. Currently, the supported Uniffle versions are `0.9.1`.

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

# Spill (Experimental)

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

# High-Bandwidth Memory (HBM) support

Gluten supports allocating memory on HBM. This feature is optional and is disabled by default. It is implemented on top of [Memkind library](http://memkind.github.io/memkind/). You can refer to memkind's [readme](https://github.com/memkind/memkind#memkind) for more details.

## Build Gluten with HBM

Gluten will internally build and link to a specific version of Memkind library and [hwloc](https://github.com/open-mpi/hwloc). Other dependencies should be installed on Driver and Worker node first:

```bash
sudo apt install -y autoconf automake g++ libnuma-dev libtool numactl unzip libdaxctl-dev
```

After the set-up, you can now build Gluten with HBM. Below command is used to enable this feature

```bash
cd /path/to/gluten

## The script builds four jars for spark 3.2.2, 3.3.1, 3.4.3 and 3.5.1.
./dev/buildbundle-veloxbe.sh --enable_hbm=ON
```

## Configure and enable HBM in Spark Application

At runtime, `MEMKIND_HBW_NODES` enviroment variable is detected for configuring HBM NUMA nodes. For the explaination to this variable, please refer to memkind's manual page. This can be set for all executors through spark conf, e.g. `--conf spark.executorEnv.MEMKIND_HBW_NODES=8-15`. Note that memory allocation fallback is also supported and cannot be turned off. If HBM is unavailable or fills up, the allocator will use default(DDR) memory.

# Intel® QuickAssist Technology (QAT) support

Gluten supports using Intel® QuickAssist Technology (QAT) for data compression during Spark Shuffle. It benefits from QAT Hardware-based acceleration on compression/decompression, and uses Gzip as compression format for higher compression ratio to reduce the pressure on disks and network transmission.

This feature is based on QAT driver library and [QATzip](https://github.com/intel/QATzip) library. Please manually download QAT driver for your system, and follow its README to build and install on all Driver and Worker node: [Intel® QuickAssist Technology Driver for Linux* – HW Version 2.0](https://www.intel.com/content/www/us/en/download/765501/intel-quickassist-technology-driver-for-linux-hw-version-2-0.html?wapkw=quickassist).

## Software Requirements

- Download QAT driver for your system, and follow its README to build and install on all Driver and Worker nodes: [Intel® QuickAssist Technology Driver for Linux* – HW Version 2.0](https://www.intel.com/content/www/us/en/download/765501/intel-quickassist-technology-driver-for-linux-hw-version-2-0.html?wapkw=quickassist).
- Below compression libraries need to be installed on all Driver and Worker nodes:
  - Zlib* library of version 1.2.7 or higher
  - ZSTD* library of version 1.5.4 or higher
  - LZ4* library

## Build Gluten with QAT

1. Setup ICP_ROOT environment variable to the directory where QAT driver is extracted. This environment variable is required during building Gluten and running Spark applications. It's recommended to put it in .bashrc on Driver and Worker nodes.

```bash
echo "export ICP_ROOT=/path/to/QAT_driver" >> ~/.bashrc
source ~/.bashrc

# Also set for root if running as non-root user
sudo su - 
echo "export ICP_ROOT=/path/to/QAT_driver" >> ~/.bashrc
exit
```

2. **This step is required if your application is running as Non-root user.**
   The users must be added to the 'qat' group after QAT drvier is installed. And change the amount of max locked memory for the username that is included in the group name. This can be done by specifying the limit in /etc/security/limits.conf.

```bash
sudo su -
usermod -aG qat username # need relogin to take effect

# To set 500MB add a line like this in /etc/security/limits.conf
echo "@qat - memlock 500000" >> /etc/security/limits.conf

exit
```

3. Enable huge page. This step is required to execute each time after system reboot. We recommend using systemctl to manage at system startup. You change the values for "max_huge_pages" and "max_huge_pages_per_process" to make sure there are enough resources for your workload. As for Spark applications, one process matches one executor. Within the executor, every task is allocated a maximum of 5 huge pages.

```bash
sudo su -

cat << EOF > /usr/local/bin/qat_startup.sh
#!/bin/bash
echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
rmmod usdm_drv
insmod $ICP_ROOT/build/usdm_drv.ko max_huge_pages=1024 max_huge_pages_per_process=32
EOF

chmod +x /usr/local/bin/qat_startup.sh

cat << EOF > /etc/systemd/system/qat_startup.service
[Unit]
Description=Configure QAT

[Service]
ExecStart=/usr/local/bin/qat_startup.sh

[Install]
WantedBy=multi-user.target
EOF

systemctl enable qat_startup.service
systemctl start qat_startup.service # setup immediately
systemctl status qat_startup.service

exit
```

4. After the setup, you are now ready to build Gluten with QAT. Use the command below to enable this feature:

```bash
cd /path/to/gluten

## The script builds four jars for spark 3.2.2, 3.3.1, 3.4.3 and 3.5.1.
./dev/buildbundle-veloxbe.sh --enable_qat=ON
```

## Enable QAT with Gzip/Zstd for shuffle compression

1. To offload shuffle compression into QAT, first make sure you have the right QAT configuration file at /etc/4xxx_devX.conf. We provide a [example configuration file](../qat/4x16.conf). This configuration sets up to 4 processes that can bind to 1 QAT, and each process can use up to 16 QAT DC instances.

```bash
## run as root
## Overwrite QAT configuration file.
cd /etc
for i in {0..7}; do echo "4xxx_dev$i.conf"; done | xargs -i cp -f /path/to/gluten/docs/qat/4x16.conf {}
## Restart QAT after updating configuration files.
adf_ctl restart
```

2. Check QAT status and make sure the status is up

```bash
adf_ctl status
```

The output should be like:

```
Checking status of all devices.
There is 8 QAT acceleration device(s) in the system:
 qat_dev0 - type: 4xxx,  inst_id: 0,  node_id: 0,  bsf: 0000:6b:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev1 - type: 4xxx,  inst_id: 1,  node_id: 1,  bsf: 0000:70:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev2 - type: 4xxx,  inst_id: 2,  node_id: 2,  bsf: 0000:75:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev3 - type: 4xxx,  inst_id: 3,  node_id: 3,  bsf: 0000:7a:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev4 - type: 4xxx,  inst_id: 4,  node_id: 4,  bsf: 0000:e8:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev5 - type: 4xxx,  inst_id: 5,  node_id: 5,  bsf: 0000:ed:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev6 - type: 4xxx,  inst_id: 6,  node_id: 6,  bsf: 0000:f2:00.0,  #accel: 1 #engines: 9 state: up
 qat_dev7 - type: 4xxx,  inst_id: 7,  node_id: 7,  bsf: 0000:f7:00.0,  #accel: 1 #engines: 9 state: up
```

3. Extra Gluten configurations are required when starting Spark application

```
--conf spark.gluten.sql.columnar.shuffle.codec=gzip # Valid options are gzip and zstd
--conf spark.gluten.sql.columnar.shuffle.codecBackend=qat
```

4. You can use below command to check whether QAT is working normally at run-time. The value of fw_counters should continue to increase during shuffle.

```
while :; do cat /sys/kernel/debug/qat_4xxx_0000:6b:00.0/fw_counters; sleep 1; done
```

## QAT driver references

**Documentation**

[README Text Files (README_QAT20.L.1.0.0-00021.txt)](https://downloadmirror.intel.com/765523/README_QAT20.L.1.0.0-00021.txt)

**Release Notes**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/632507/intel-quickassist-technology-intel-qat-software-for-linux-release-notes-hardware-version-2-0.html) - Release Notes for the latest changes in this release.

**Getting Started Guide**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/632506/intel-quickassist-technology-intel-qat-software-for-linux-getting-started-guide-hardware-version-2-0.html) - Getting Started Guide for detailed installation instructions.

**Programmer's Guide**

Check out the [Intel® QuickAssist Technology Software for Linux*](https://www.intel.com/content/www/us/en/content-details/743912/intel-quickassist-technology-intel-qat-software-for-linux-programmers-guide-hardware-version-2-0.html) - Programmer's Guide for software usage guidelines.

For more Intel® QuickAssist Technology resources go to [Intel® QuickAssist Technology (Intel® QAT)](https://developer.intel.com/quickassist)

# Intel® In-memory Analytics Accelerator (IAA/IAX) support

Similar to Intel® QAT, Gluten supports using Intel® In-memory Analytics Accelerator (IAA, also called IAX) for data compression during Spark Shuffle. It benefits from IAA Hardware-based acceleration on compression/decompression, and uses Gzip as compression format for higher compression ratio to reduce the pressure on disks and network transmission.

This feature is based on Intel® [QPL](https://github.com/intel/qpl).

## Build Gluten with IAA

Gluten will internally build and link to a specific version of QPL library, but extra environment setup is still required. Please refer to [QPL Installation Guide](https://intel.github.io/qpl/documentation/get_started_docs/installation.html) to install dependencies and configure accelerators.

**This step is required if your application is running as Non-root user.**
Create a group for the users who have privilege to use IAA, and grant group iaa read/write access to the IAA Work-Queues.

```bash
sudo groupadd iaa
sudo usermod -aG iaa username # need to relogin
sudo chgrp -R iaa /dev/iax
sudo chmod -R g+rw /dev/iax
```

After the set-up, you can now build Gluten with QAT. Below command is used to enable this feature

```bash
cd /path/to/gluten

## The script builds four jars for spark 3.2.2, 3.3.1, 3.4.3 and 3.5.1.
./dev/buildbundle-veloxbe.sh --enable_iaa=ON
```

## Enable IAA with Gzip Compression for shuffle compression

1. To enable QAT at run-time, first make sure you have configured the IAA Work-Queues correctly, and the file permissions of /dev/iax/wqX.0 are correct.

```bash
sudo ls -l /dev/iax
```

The output should be like:

```
total 0
crw-rw---- 1 root iaa 509, 0 Apr  5 18:54 wq1.0
crw-rw---- 1 root iaa 509, 5 Apr  5 18:54 wq11.0
crw-rw---- 1 root iaa 509, 6 Apr  5 18:54 wq13.0
crw-rw---- 1 root iaa 509, 7 Apr  5 18:54 wq15.0
crw-rw---- 1 root iaa 509, 1 Apr  5 18:54 wq3.0
crw-rw---- 1 root iaa 509, 2 Apr  5 18:54 wq5.0
crw-rw---- 1 root iaa 509, 3 Apr  5 18:54 wq7.0
crw-rw---- 1 root iaa 509, 4 Apr  5 18:54 wq9.0
```

2. Extra Gluten configurations are required when starting Spark application

```
--conf spark.gluten.sql.columnar.shuffle.codec=gzip
--conf spark.gluten.sql.columnar.shuffle.codecBackend=iaa
```

## IAA references

**Intel® IAA Enabling Guide**

Check out the [Intel® In-Memory Analytics Accelerator (Intel® IAA) Enabling Guide](https://www.intel.com/content/www/us/en/developer/articles/technical/intel-iaa-enabling-guide.html)

**Intel® QPL Documentation**

Check out the [Intel® Query Processing Library (Intel® QPL) Documentation](https://intel.github.io/qpl/index.html)

# Test TPC-H or TPC-DS on Gluten with Velox backend

All TPC-H and TPC-DS queries are supported in Gluten Velox backend.

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

# External reference setup

TO ease your first-hand experience of using Gluten, we have set up an external reference cluster. If you are interested, please contact Weiting.Chen@intel.com.

# Gluten UI

## Gluten event

Gluten provides two events `GlutenBuildInfoEvent` and `GlutenPlanFallbackEvent`:

- GlutenBuildInfoEvent, it contains the Gluten build information so that we are able to be aware of the environment when doing some debug.
  It includes `Java Version`, `Scala Version`, `GCC Version`, `Gluten Version`, `Spark Version`, `Hadoop Version`, `Gluten Revision`, `Backend`, `Backend Revision`, etc.

- GlutenPlanFallbackEvent, it contains the fallback information for each query execution.
  Note, if the query execution is in AQE, then Gluten will post it for each stage.

Developers can register `SparkListener` to handle these two Gluten events.

## SQL tab

Gluten provides a tab based on Spark UI, named `Gluten SQL / DataFrame`

![Gluten-UI](../image/gluten-ui.png)

This tab contains two parts:

1. The Gluten build information.
2. SQL/Dataframe queries fallback information.

If you want to disable Gluten UI, add a config when submitting `--conf spark.gluten.ui.enabled=false`.

## History server

Gluten UI also supports Spark history server. Add gluten-ui jar into the history server classpath, e.g., $SPARK_HOME/jars, then restart history server.

## Native plan string

Gluten supports inject native plan string into Spark explain with formatted mode by setting `--conf spark.gluten.sql.injectNativePlanStringToExplain=true`.
Here is an example, how Gluten show the native plan string.

```
(9) WholeStageCodegenTransformer (2)
Input [6]: [c1#0L, c2#1L, c3#2L, c1#3L, c2#4L, c3#5L]
Arguments: false
Native Plan:
-- Project[expressions: (n3_6:BIGINT, "n0_0"), (n3_7:BIGINT, "n0_1"), (n3_8:BIGINT, "n0_2"), (n3_9:BIGINT, "n1_0"), (n3_10:BIGINT, "n1_1"), (n3_11:BIGINT, "n1_2")] -> n3_6:BIGINT, n3_7:BIGINT, n3_8:BIGINT, n3_9:BIGINT, n3_10:BIGINT, n3_11:BIGINT
  -- HashJoin[INNER n1_1=n0_1] -> n1_0:BIGINT, n1_1:BIGINT, n1_2:BIGINT, n0_0:BIGINT, n0_1:BIGINT, n0_2:BIGINT
    -- TableScan[table: hive_table, range filters: [(c2, Filter(IsNotNull, deterministic, null not allowed))]] -> n1_0:BIGINT, n1_1:BIGINT, n1_2:BIGINT
    -- ValueStream[] -> n0_0:BIGINT, n0_1:BIGINT, n0_2:BIGINT
```

## Native plan with stats

Gluten supports print native plan with stats to executor system output stream by setting `--conf spark.gluten.sql.debug=true`.
Note that, the plan string with stats is task level which may cause executor log size big. Here is an example, how Gluten show the native plan string with stats.

```
I20231121 10:19:42.348845 90094332 WholeStageResultIterator.cc:220] Native Plan with stats for: [Stage: 1 TID: 16]
-- Project[expressions: (n3_6:BIGINT, "n0_0"), (n3_7:BIGINT, "n0_1"), (n3_8:BIGINT, "n0_2"), (n3_9:BIGINT, "n1_0"), (n3_10:BIGINT, "n1_1"), (n3_11:BIGINT, "n1_2")] -> n3_6:BIGINT, n3_7:BIGINT, n3_8:BIGINT, n3_9:BIGINT, n3_10:BIGINT, n3_11:BIGINT
   Output: 27 rows (3.56KB, 3 batches), Cpu time: 10.58us, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1
      queuedWallNanos              sum: 2.00us, count: 1, min: 2.00us, max: 2.00us
      runningAddInputWallNanos     sum: 626ns, count: 1, min: 626ns, max: 626ns
      runningFinishWallNanos       sum: 0ns, count: 1, min: 0ns, max: 0ns
      runningGetOutputWallNanos    sum: 5.54us, count: 1, min: 5.54us, max: 5.54us
  -- HashJoin[INNER n1_1=n0_1] -> n1_0:BIGINT, n1_1:BIGINT, n1_2:BIGINT, n0_0:BIGINT, n0_1:BIGINT, n0_2:BIGINT
     Output: 27 rows (3.56KB, 3 batches), Cpu time: 223.00us, Blocked wall time: 0ns, Peak memory: 93.12KB, Memory allocations: 15
     HashBuild: Input: 10 rows (960B, 10 batches), Output: 0 rows (0B, 0 batches), Cpu time: 185.67us, Blocked wall time: 0ns, Peak memory: 68.00KB, Memory allocations: 2, Threads: 1
        distinctKey0                 sum: 4, count: 1, min: 4, max: 4
        hashtable.capacity           sum: 4, count: 1, min: 4, max: 4
        hashtable.numDistinct        sum: 10, count: 1, min: 10, max: 10
        hashtable.numRehashes        sum: 1, count: 1, min: 1, max: 1
        queuedWallNanos              sum: 0ns, count: 1, min: 0ns, max: 0ns
        rangeKey0                    sum: 4, count: 1, min: 4, max: 4
        runningAddInputWallNanos     sum: 1.27ms, count: 1, min: 1.27ms, max: 1.27ms
        runningFinishWallNanos       sum: 0ns, count: 1, min: 0ns, max: 0ns
        runningGetOutputWallNanos    sum: 1.29us, count: 1, min: 1.29us, max: 1.29us
     H23/11/21 10:19:42 INFO TaskSetManager: Finished task 3.0 in stage 1.0 (TID 13) in 335 ms on 10.221.97.35 (executor driver) (1/10)
ashProbe: Input: 9 rows (864B, 3 batches), Output: 27 rows (3.56KB, 3 batches), Cpu time: 37.33us, Blocked wall time: 0ns, Peak memory: 25.12KB, Memory allocations: 13, Threads: 1
        dynamicFiltersProduced       sum: 1, count: 1, min: 1, max: 1
        queuedWallNanos              sum: 0ns, count: 1, min: 0ns, max: 0ns
        runningAddInputWallNanos     sum: 4.54us, count: 1, min: 4.54us, max: 4.54us
        runningFinishWallNanos       sum: 83ns, count: 1, min: 83ns, max: 83ns
        runningGetOutputWallNanos    sum: 29.08us, count: 1, min: 29.08us, max: 29.08us
    -- TableScan[table: hive_table, range filters: [(c2, Filter(IsNotNull, deterministic, null not allowed))]] -> n1_0:BIGINT, n1_1:BIGINT, n1_2:BIGINT
       Input: 9 rows (864B, 3 batches), Output: 9 rows (864B, 3 batches), Cpu time: 630.75us, Blocked wall time: 0ns, Peak memory: 2.44KB, Memory allocations: 63, Threads: 1, Splits: 3
          dataSourceWallNanos              sum: 102.00us, count: 1, min: 102.00us, max: 102.00us
          dynamicFiltersAccepted           sum: 1, count: 1, min: 1, max: 1
          flattenStringDictionaryValues    sum: 0, count: 1, min: 0, max: 0
          ioWaitNanos                      sum: 312.00us, count: 1, min: 312.00us, max: 312.00us
          localReadBytes                   sum: 0B, count: 1, min: 0B, max: 0B
          numLocalRead                     sum: 0, count: 1, min: 0, max: 0
          numPrefetch                      sum: 0, count: 1, min: 0, max: 0
          numRamRead                       sum: 0, count: 1, min: 0, max: 0
          numStorageRead                   sum: 6, count: 1, min: 6, max: 6
          overreadBytes                    sum: 0B, count: 1, min: 0B, max: 0B
          prefetchBytes                    sum: 0B, count: 1, min: 0B, max: 0B
          queryThreadIoLatency             sum: 12, count: 1, min: 12, max: 12
          ramReadBytes                     sum: 0B, count: 1, min: 0B, max: 0B
          runningAddInputWallNanos         sum: 0ns, count: 1, min: 0ns, max: 0ns
          runningFinishWallNanos           sum: 125ns, count: 1, min: 125ns, max: 125ns
          runningGetOutputWallNanos        sum: 1.07ms, count: 1, min: 1.07ms, max: 1.07ms
          skippedSplitBytes                sum: 0B, count: 1, min: 0B, max: 0B
          skippedSplits                    sum: 0, count: 1, min: 0, max: 0
          skippedStrides                   sum: 0, count: 1, min: 0, max: 0
          storageReadBytes                 sum: 3.44KB, count: 1, min: 3.44KB, max: 3.44KB
          totalScanTime                    sum: 0ns, count: 1, min: 0ns, max: 0ns
    -- ValueStream[] -> n0_0:BIGINT, n0_1:BIGINT, n0_2:BIGINT
       Input: 0 rows (0B, 0 batches), Output: 10 rows (960B, 10 batches), Cpu time: 1.03ms, Blocked wall time: 0ns, Peak memory: 0B, Memory allocations: 0, Threads: 1
          runningAddInputWallNanos     sum: 0ns, count: 1, min: 0ns, max: 0ns
          runningFinishWallNanos       sum: 54.62us, count: 1, min: 54.62us, max: 54.62us
          runningGetOutputWallNanos    sum: 1.10ms, count: 1, min: 1.10ms, max: 1.10ms
```

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
