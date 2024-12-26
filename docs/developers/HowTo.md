---
layout: page
title: How To Use Gluten
nav_order: 1
parent: Developer Overview
---
There are some common questions about developing, debugging and testing been asked again and again. In order to help the developers to contribute
to Gluten as soon as possible, we collected these frequently asked questions, and organized them in the form of Q&A. It's convenient for the developers
to check and learn.

When you encountered a new problem and then resolved it, please add a new item to this document if you think it may be helpful to the other developers.

We use `${GLUTEN_HOME}` to represent the home directory of Gluten in this document.

# How to understand the key work of Gluten?

The Gluten worked as the role of bridge, it's a middle layer between the Spark and the native execution library.

The Gluten is responsible for validating whether the operators of the Spark plan can be executed by the native engine or not. If yes, the Gluten
transforms Spark plan to Substrait plan, and then send the Substrait plan to the native engine.

The Gluten codes consist of two parts: the C++ codes and the Java/Scala codes. 
1. All C++ codes are placed under the directory of `${GLUTEN_HOME}/cpp`, the Java/Scala codes are placed under several directories, such as
  `${GLUTEN_HOME}/gluten-substrait` `${GLUTEN_HOME}/gluten-data` `${GLUTEN_HOME}/backends-velox`.
2. The Java/Scala codes are responsible for validating and transforming the execution plan. Source data should also be provided, the source data may
  come from files or other forms such as networks.
3. The C++ codes take the Substrait plan and the source data as inputs and transform the Substrait plan to the corresponding backend plan. If the backend
  is Velox, the Substrait plan will be transformed to the Velox plan, and then be executed.

JNI is a programming technology of invoking C++ from Java. All JNI interfaces are defined in the file `JniWrapper.cc` under the directory `jni`.

# How to debug in Gluten?

## 1 How to debug C++
If you don't concern about the Scala/Java codes and just want to debug the C++ codes executed in native engine, you may debug the C++ via benchmarks
with GDB.

To debug C++, you have to generate the example files, the example files consist of:
- A file contained Substrait plan in JSON format
- One or more input data files in Parquet format

You can generate the example files by the following steps:

1. Build Velox and Gluten CPP:

```
${GLUTEN_HOME}/dev/builddeps-veloxbe.sh --build_tests=ON --build_benchmarks=ON --build_type=Debug
```

- Compiling with `--build_type=Debug` is good for debugging.
- The executable file `generic_benchmark` will be generated under the directory of `gluten_home/cpp/build/velox/benchmarks/`.

2. Build Gluten and generate the example files:

```
cd ${GLUTEN_HOME}
mvn clean package -Pspark-3.2 -Pbackends-velox -Pceleborn -Puniffle
mvn test -Pspark-3.2 -Pbackends-velox -Pceleborn -pl backends-velox \
-am -DtagsToInclude="org.apache.gluten.tags.GenerateExample" \
-Dtest=none -DfailIfNoTests=false \
-Dexec.skip
```

- After the above operations, the example files are generated under `${GLUTEN_HOME}/backends-velox`
- You can check it by the command `tree ${GLUTEN_HOME}/backends-velox/generated-native-benchmark/`
- You may replace `-Pspark-3.2` with `-Pspark-3.3` if your spark's version is 3.3

```shell
$ tree ${GLUTEN_HOME}/backends-velox/generated-native-benchmark/
/some-dir-to-gluten-home/backends-velox/generated-native-benchmark/
├── example.json
├── example_lineitem
│   ├── part-00000-3ec19189-d20e-4240-85ae-88631d46b612-c000.snappy.parquet
│   └── _SUCCESS
└── example_orders
    ├── part-00000-1e66fb98-4dd6-47a6-8679-8625dbc437ee-c000.snappy.parquet
    └── _SUCCESS
```

3. Now, run benchmarks with GDB

```shell
cd ${GLUTEN_HOME}/cpp/build/velox/benchmarks/
gdb generic_benchmark
```

- When GDB load `generic_benchmark` successfully, you can set `breakpoint` on the `main` function with command `b main`, and then run with command `r`,
  then the process `generic_benchmark` will start and stop at the `main` function.
- You can check the variables' state with command `p variable_name`, or execute the program line by line with command `n`, or step-in the function been
  called with command `s`.
- Actually, you can debug `generic_benchmark` with any gdb commands as debugging normal C++ program, because the `generic_benchmark` is a pure C++
  executable file in fact.

4. `gdb-tui` is a valuable feature and is worth trying. You can get more help from the online docs.
[gdb-tui](https://sourceware.org/gdb/onlinedocs/gdb/TUI.html)

5. You can start `generic_benchmark` with specific JSON plan and input files
- If you omit them, the `example.json, example_lineitem + example_orders` under the directory of `${GLUTEN_HOME}/backends-velox/generated-native-benchmark`
  will be used as default.
- You can also edit the file `example.json` to custom the Substrait plan or specify the inputs files placed in the other directory.

6. Get more detail information about benchmarks from [MicroBenchmarks](./MicroBenchmarks.md)

## 2 How to debug plan validation process

Gluten will validate generated plan before execute it, and validation usually happens in native side, so we provide a utility to help debug validation process in native side.

1. Run query with conf `spark.gluten.sql.debug=true`, and you will find generated plan be printed in stderr with json format, save it as `plan.json` for example.
2. Compile cpp part with `--build_benchmarks=ON`, then check `plan_validator_util` executable file in `${GLUTEN_HOME}/cpp/build/velox/benchmarks/`.
3. Run or debug with `./plan_validator_util <path>/plan.json`

## 3 How to debug Java/Scala
wait to add

## 4 How to debug with core-dump
wait to complete

```shell
cd the_directory_of_core_file_generated
gdb ${GLUTEN_HOME}/cpp/build/releases/libgluten.so 'core-Executor task l-2000883-1671542526'

```
- the `core-Executor task l-2000883-1671542526` represents the core file name.

# How to use jemalloc for Gluten native engine

Currently, we have no dedicated memory allocator implemented by jemalloc. User can set environment variable `LD_PRELOAD` for lib jemalloc
to let it override the corresponding C standard functions entirely. It may help alleviate OOM issues.

`spark.executorEnv.LD_PREALOD=/path/to/libjemalloc.so`

# How to run TPC-H on Velox backend

Now, both Parquet and DWRF format files are supported, related scripts and files are under the directory of `${GLUTEN_HOME}/backends-velox/workload/tpch`.
The file `README.md` under `${GLUTEN_HOME}/backends-velox/workload/tpch` offers some useful help, but it's still not enough and exact.

One way of run TPC-H test is to run velox-be by workflow, you can refer to [velox_backend.yml](https://github.com/apache/incubator-gluten/blob/main/.github/workflows/velox_backend.yml#L280)

Here we will explain how to run TPC-H on Velox backend with the Parquet file format.
1. First, prepare the datasets, you have two choices.
  - One way, generate Parquet datasets using the script under `${GLUTEN_HOME}/tools/workload/tpch/gen_data/parquet_dataset`, you can get help from the above
    -mentioned `README.md`.
  - The other way, using the small dataset under `${GLUTEN_HOME}/backends-velox/src/test/resources/tpch-data-parquet` directly, if you just want to make simple
    TPC-H testing, this dataset is a good choice.
2. Second, run TPC-H on Velox backend testing.
  - Modify `${GLUTEN_HOME}/tools/workload/tpch/run_tpch/tpch_parquet.scala`.
    - Set `var parquet_file_path` to correct directory. If using the small dataset directly in the step one, then modify it as below:

    ```scala
    var parquet_file_path = "gluten_home/backends-velox/src/test/resources/tpch-data-parquet"
    ```

    - Set `var gluten_root` to correct directory. If `${GLUTEN_HOME}` is the directory of `/home/gluten`, then modify it as below

    ```scala
    var gluten_root = "/home/gluten"
    ```

  - Modify `${GLUTEN_HOME}/tools/workload/tpch/run_tpch/tpch_parquet.sh`.
    - Set `GLUTEN_JAR` correctly. Please refer to the section of [Build Gluten with Velox Backend](../get-started/Velox.md/#2-build-gluten-with-velox-backend)
    - Set `SPARK_HOME` correctly.
    - Set the memory configurations appropriately.
  - Execute `tpch_parquet.sh` using the below command.
    - `cd ${GLUTEN_HOME}/tools/workload/tpch/run_tpch/`
    - `./tpch_parquet.sh`

# How to run TPC-DS

wait to add

# How to track the memory exhaust problem

When your gluten spark jobs failed because of OOM, you can track the memory allocation's call stack by configuring `spark.gluten.memory.backtrace.allocation = true`.
The above configuration will use `BacktraceAllocationListener` wrapping from `SparkAllocationListener` to create `VeloxMemoryManager`.

`BacktraceAllocationListener` will check every allocation, if a single allocation bytes exceeds a fixed value or the accumulative allocation bytes exceeds 1/2/3...G,
the call stack of memory allocation will be outputted to standard output, you can check the backtrace and get some valuable information about tracking the memory exhaust issues.

You can also adjust the policy to decide when to backtrace, such as the fixed value.

