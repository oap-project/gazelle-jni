/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.utils.clickhouse

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.GlutenSortShuffleSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector._
import org.apache.spark.sql.errors.{GlutenQueryCompilationErrorsDSv2Suite, GlutenQueryCompilationErrorsSuite, GlutenQueryExecutionErrorsSuite, GlutenQueryParsingErrorsSuite}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.binaryfile.GlutenBinaryFileFormatSuite
import org.apache.spark.sql.execution.datasources.csv.{GlutenCSVLegacyTimeParserSuite, GlutenCSVv1Suite, GlutenCSVv2Suite}
import org.apache.spark.sql.execution.datasources.exchange.GlutenValidateRequirementsSuite
import org.apache.spark.sql.execution.datasources.json.{GlutenJsonLegacyTimeParserSuite, GlutenJsonV1Suite, GlutenJsonV2Suite}
import org.apache.spark.sql.execution.datasources.orc._
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.text.{GlutenTextV1Suite, GlutenTextV2Suite}
import org.apache.spark.sql.execution.datasources.v2.{GlutenDataSourceV2StrategySuite, GlutenFileTableSuite, GlutenV2PredicateSuite}
import org.apache.spark.sql.execution.exchange.GlutenEnsureRequirementsSuite
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.extension.{GlutenCollapseProjectExecTransformerSuite, GlutenSessionExtensionSuite, TestFileSourceScanExecTransformer}
import org.apache.spark.sql.gluten.GlutenFallbackSuite
import org.apache.spark.sql.hive.execution.GlutenHiveSQLQueryCHSuite
import org.apache.spark.sql.sources._

// Some settings' line length exceeds 100
// scalastyle:off line.size.limit

class ClickHouseTestSettings extends BackendTestSettings {

  enableSuite[GlutenStringFunctionsSuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
  enableSuite[GlutenBloomFilterAggregateQuerySuiteCGOff]
  enableSuite[GlutenDataSourceV2DataFrameSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2DataFrameSuite]
  enableSuite[GlutenDataSourceV2FunctionSuite]
  enableSuite[GlutenDataSourceV2SQLSessionCatalogSuite]
  enableSuite[GlutenDataSourceV2SQLSuiteV1Filter]
  enableSuite[GlutenDataSourceV2SQLSuiteV2Filter]
  enableSuite[GlutenDataSourceV2Suite]
    // Rewrite the following tests in GlutenDataSourceV2Suite.
    .exclude("partitioning reporting")
    .exclude("ordering and partitioning reporting")
  enableSuite[GlutenDeleteFromTableSuite]
  enableSuite[GlutenFileDataSourceV2FallBackSuite]
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("Fallback Parquet V2 to V1")
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    // NEW SUITE: disable as they check vanilla spark plan
    .exclude("partitioned join: number of buckets mismatch should trigger shuffle")
    .exclude("partitioned join: only one side reports partitioning")
    .exclude("partitioned join: join with two partition keys and different # of partition keys")
    // disable due to check for SMJ node
    .excludeByPrefix("SPARK-41413: partitioned join:")
    .excludeByPrefix("SPARK-42038: partially clustered:")
    .exclude("SPARK-44641: duplicated records when SPJ is not triggered")
  enableSuite[GlutenLocalScanSuite]
  enableSuite[GlutenMetadataColumnSuite]
  enableSuite[GlutenSupportsCatalogOptionsSuite]
  enableSuite[GlutenTableCapabilityCheckSuite]
  enableSuite[GlutenWriteDistributionAndOrderingSuite]
  enableSuite[GlutenQueryCompilationErrorsDSv2Suite]
  enableSuite[GlutenQueryCompilationErrorsSuite]
  enableSuite[GlutenQueryExecutionErrorsSuite]
    // NEW SUITE: disable as it expects exception which doesn't happen when offloaded to gluten
    .exclude(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION: compatibility with Spark 2.4/3.2 in reading/writing dates")
    // Doesn't support unhex with failOnError=true.
    .exclude("CONVERSION_INVALID_INPUT: to_binary conversion function hex")
  enableSuite[GlutenQueryParsingErrorsSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("SPARK-45786: Decimal multiply, divide, remainder, quot")
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    // Set timezone through config.
    .exclude("data type casting")
  enableSuite[GlutenTryCastSuite]
    .exclude(
      "Process Infinity, -Infinity, NaN in case insensitive manner" // +inf not supported in folly.
    )
    .exclude("ANSI mode: Throw exception on casting out-of-range value to byte type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to short type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to int type")
    .exclude("ANSI mode: Throw exception on casting out-of-range value to long type")
    .exclude("cast from invalid string to numeric should throw NumberFormatException")
    .exclude("SPARK-26218: Fix the corner case of codegen when casting float to Integer")
    // Set timezone through config.
    .exclude("data type casting")
  enableSuite[GlutenCollectionExpressionsSuite]
    // Rewrite in Gluten to replace Seq with Array
    .exclude("Shuffle")
    .excludeGlutenTest("Shuffle")
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenDateExpressionsSuite]
    // Has exception in fallback execution when we use resultDF.collect in evaluation.
    .exclude("TIMESTAMP_MICROS")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("to_unix_timestamp")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("Hour")
    // Unsupported format: yyyy-MM-dd HH:mm:ss.SSS
    .exclude("SPARK-33498: GetTimestamp,UnixTimestamp,ToUnixTimestamp with parseError")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("DateFormat")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("to_timestamp exception mode")
    // Replaced by a gluten test to pass timezone through config.
    .exclude("from_unixtime")
    // https://github.com/facebookincubator/velox/pull/10563/files#diff-140dc50e6dac735f72d29014da44b045509df0dd1737f458de1fe8cfd33d8145
    .excludeGlutenTest("from_unixtime")
  enableSuite[GlutenDecimalExpressionSuite]
  enableSuite[GlutenDecimalPrecisionSuite]
  enableSuite[GlutenHashExpressionsSuite]
  enableSuite[GlutenHigherOrderFunctionsSuite]
  enableSuite[GlutenIntervalExpressionsSuite]
  enableSuite[GlutenJsonExpressionsSuite]
    // https://github.com/apache/incubator-gluten/issues/8102
    .exclude("$.store.book")
    .exclude("$")
    .exclude("$.store.book[0]")
    .exclude("$.store.book[*]")
    .exclude("$.store.book[*].category")
    .exclude("$.store.book[*].isbn")
    .exclude("$.store.book[*].reader")
    .exclude("$.store.basket[*]")
    .exclude("$.store.basket[*][0]")
    .exclude("$.store.basket[0][*]")
    .exclude("$.store.basket[*][*]")
    .exclude("$.store.basket[0][*].b")
    // Exception class different.
    .exclude("from_json - invalid data")
  enableSuite[GlutenJsonFunctionsSuite]
    // * in get_json_object expression not supported in velox
    .exclude("SPARK-42782: Hive compatibility check for get_json_object")
    // Velox does not support single quotes in get_json_object function.
    .exclude("function get_json_object - support single quotes")
  enableSuite[GlutenLiteralExpressionSuite]
    .exclude("default")
    // FIXME(yma11): ObjectType is not covered in RowEncoder/Serializer in vanilla spark
    .exclude("SPARK-37967: Literal.create support ObjectType")
  enableSuite[GlutenMathExpressionsSuite]
    // Spark round UT for round(3.1415,3) is not correct.
    .exclude("round/bround/floor/ceil")
  enableSuite[GlutenMiscExpressionsSuite]
  enableSuite[GlutenNondeterministicSuite]
    .exclude("MonotonicallyIncreasingID")
    .exclude("SparkPartitionID")
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]
  enableSuite[GlutenRandomSuite]
    .exclude("random")
    .exclude("SPARK-9127 codegen with long seed")
  enableSuite[GlutenRegexpExpressionsSuite]
  enableSuite[GlutenSortShuffleSuite]
  enableSuite[GlutenSortOrderExpressionsSuite]
  enableSuite[GlutenStringExpressionsSuite]
  enableSuite[GlutenTryEvalSuite]
  enableSuite[GlutenBinaryFileFormatSuite]
    // Exception.
    .exclude("column pruning - non-readable file")
  enableSuite[GlutenCSVv1Suite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
  enableSuite[GlutenCSVv2Suite]
    .exclude("Gluten - test for FAILFAST parsing mode")
    // Rule org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown in batch
    // Early Filter and Projection Push-Down generated an invalid plan
    .exclude("SPARK-26208: write and read empty data to csv file with headers")
    // file cars.csv include null string, Arrow not support to read
    .exclude("old csv data source name works")
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
  enableSuite[GlutenCSVLegacyTimeParserSuite]
    // file cars.csv include null string, Arrow not support to read
    .exclude("DDL test with schema")
    .exclude("save csv")
    .exclude("save csv with compression codec option")
    .exclude("save csv with empty fields with user defined empty values")
    .exclude("save csv with quote")
    .exclude("SPARK-13543 Write the output as uncompressed via option()")
    // Arrow not support corrupt record
    .exclude("SPARK-27873: disabling enforceSchema should not fail columnNameOfCorruptRecord")
    .exclude("DDL test with tab separated file")
    .exclude("DDL test parsing decimal type")
    .exclude("test with tab delimiter and double quote")
    // varchar
    .exclude("SPARK-48241: CSV parsing failure with char/varchar type columns")
  enableSuite[GlutenJsonV1Suite]
    // FIXME: Array direct selection fails
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenJsonV2Suite]
    // exception test
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenJsonLegacyTimeParserSuite]
    .exclude("Complex field and type inferring")
    .exclude("SPARK-4228 DataFrame to JSON")
  enableSuite[GlutenValidateRequirementsSuite]
  enableSuite[GlutenOrcColumnarBatchReaderSuite]
  enableSuite[GlutenOrcFilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcPartitionDiscoverySuite]
    .exclude("read partitioned table - normal case")
    .exclude("read partitioned table - with nulls")
  enableSuite[GlutenOrcV1PartitionDiscoverySuite]
    .exclude("read partitioned table - normal case")
    .exclude("read partitioned table - with nulls")
    .exclude("read partitioned table - partition key included in orc file")
    .exclude("read partitioned table - with nulls and partition keys are included in Orc file")
  enableSuite[GlutenOrcV1QuerySuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("Simple selection form ORC table")
    .exclude("simple select queries")
    .exclude("overwriting")
    .exclude("self-join")
    .exclude("columns only referenced by pushed down filters should remain")
    .exclude("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
    .exclude("Read/write binary data")
    .exclude("Read/write all types with non-primitive type")
    .exclude("Creating case class RDD table")
    .exclude("save and load case class RDD with `None`s as orc")
    .exclude("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when" +
      " compression is unset")
    .exclude("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .exclude("appending")
    .exclude("nested data - struct with array field")
    .exclude("nested data - array of struct")
    .exclude("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .exclude("SPARK-10623 Enable ORC PPD")
    .exclude("SPARK-14962 Produce correct results on array type with isnotnull")
    .exclude("SPARK-15198 Support for pushing down filters for boolean types")
    .exclude("Support for pushing down filters for decimal types")
    .exclude("Support for pushing down filters for timestamp types")
    .exclude("column nullability and comment - write and then read")
    .exclude("Empty schema does not read data from ORC file")
    .exclude("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("LZO compression options for writing to an ORC file")
    .exclude("Schema discovery on empty ORC files")
    .exclude("SPARK-21791 ORC should support column names with dot")
    .exclude("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("Read/write all timestamp types")
    .exclude("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .exclude("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
  enableSuite[GlutenOrcV2QuerySuite]
    .exclude("Read/write binary data")
    .exclude("Read/write all types with non-primitive type")
    // Rewrite to disable Spark's columnar reader.
    .exclude("Simple selection form ORC table")
    .exclude("Creating case class RDD table")
    .exclude("save and load case class RDD with `None`s as orc")
    .exclude("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when compression is unset")
    .exclude("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)")
    .exclude("appending")
    .exclude("nested data - struct with array field")
    .exclude("nested data - array of struct")
    .exclude("SPARK-9170: Don't implicitly lowercase of user-provided columns")
    .exclude("SPARK-10623 Enable ORC PPD")
    .exclude("SPARK-14962 Produce correct results on array type with isnotnull")
    .exclude("SPARK-15198 Support for pushing down filters for boolean types")
    .exclude("Support for pushing down filters for decimal types")
    .exclude("Support for pushing down filters for timestamp types")
    .exclude("column nullability and comment - write and then read")
    .exclude("Empty schema does not read data from ORC file")
    .exclude("read from multiple orc input paths")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC")
    .exclude("LZO compression options for writing to an ORC file")
    .exclude("Schema discovery on empty ORC files")
    .exclude("SPARK-21791 ORC should support column names with dot")
    .exclude("SPARK-25579 ORC PPD should support column names with dot")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    .exclude("SPARK-37728: Reading nested columns with ORC vectorized reader should not")
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("Read/write all timestamp types")
    .exclude("SPARK-37463: read/write Timestamp ntz to Orc with different time zone")
    .exclude("SPARK-39381: Make vectorized orc columar writer batch size configurable")
    .exclude("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE")
    .exclude("simple select queries")
    .exclude("overwriting")
    .exclude("self-join")
    .exclude("columns only referenced by pushed down filters should remain")
    .exclude("SPARK-5309 strings stored using dictionary compression in orc")
    // For exception test.
    .exclude("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core")
  enableSuite[GlutenOrcSourceSuite]
    // Rewrite to disable Spark's columnar reader.
    .exclude("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .exclude("SPARK-31238, SPARK-31423: rebasing dates in write")
    .exclude("SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .exclude("SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .exclude("SPARK-34862: Support ORC vectorized reader for nested column")
    // Ignored to disable vectorized reading check.
    .exclude("SPARK-36594: ORC vectorized reader should properly check maximal number of fields")
    .exclude("create temporary orc table")
    .exclude("create temporary orc table as")
    .exclude("appending insert")
    .exclude("overwrite insert")
    .exclude("SPARK-34897: Support reconcile schemas based on index after nested column pruning")
    .excludeGlutenTest("SPARK-31238: compatibility with Spark 2.4 in reading dates")
    .excludeGlutenTest("SPARK-31238, SPARK-31423: rebasing dates in write")
    .excludeGlutenTest("SPARK-34862: Support ORC vectorized reader for nested column")
    // exclude as struct not supported
    .exclude("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle a column name which consists of only numbers")
    .exclude("SPARK-37812: Reuse result row when deserializing a struct")
    // rewrite
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .exclude("SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=true, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenOrcV1FilterSuite]
    .exclude("SPARK-32622: case sensitivity in predicate pushdown")
  enableSuite[GlutenOrcV1SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - with partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
    // Vectorized reading.
    .exclude("Spark vectorized reader - without partition data column - " +
      "select only expressions without references")
    .exclude("Spark vectorized reader - with partition data column - " +
      "select only expressions without references")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field")
    .exclude(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - without partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - with partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - without partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - with partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function")
    .exclude(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Sort")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Expand")
    .exclude(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
  enableSuite[GlutenOrcV2SchemaPruningSuite]
    .exclude(
      "Spark vectorized reader - without partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - with partition data column - select only top-level fields")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after join")
    .exclude("Spark vectorized reader - " +
      "without partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - " +
      "with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field with disabled nested schema pruning")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field with disabled nested schema pruning")
    .exclude(
      "Spark vectorized reader - without partition data column - select a single complex field")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field")
    .exclude(
      "Non-vectorized reader - without partition data column - select a single complex field")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and its parent struct")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and its parent struct")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field array and its parent struct array")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field array and its parent struct array")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field from a map entry and its parent map entry")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and the partition column")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and the partition column")
    .exclude("Spark vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Spark vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - without partition data column - partial schema intersection - select missing subfield")
    .exclude("Non-vectorized reader - with partition data column - partial schema intersection - select missing subfield")
    .exclude(
      "Spark vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude(
      "Non-vectorized reader - without partition data column - no unnecessary schema pruning")
    .exclude("Non-vectorized reader - with partition data column - no unnecessary schema pruning")
    .exclude("Spark vectorized reader - without partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - with partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - without partition data column - empty schema intersection")
    .exclude("Non-vectorized reader - with partition data column - empty schema intersection")
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and is null expression in project")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and is null expression in project")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map key using map_keys")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map key using map_keys")
    .exclude("Spark vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - without partition data column - select nested field from a complex map value using map_values")
    .exclude("Non-vectorized reader - with partition data column - select nested field from a complex map value using map_values")
    .exclude("Spark vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Spark vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - with partition data column - select explode of nested field of array of struct")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after join")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after outer join")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after outer join")
    .exclude("Spark vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - without partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Non-vectorized reader - with partition data column - select nested field in aggregation function of Aggregate")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function")
    .exclude(
      "Non-vectorized reader - with partition data column - select nested field in window function")
    .exclude("Spark vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Spark vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - without partition data column - select nested field in window function and then order by")
    .exclude("Non-vectorized reader - with partition data column - select nested field in window function and then order by")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - without partition data column - select nested field in Sort")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Sort")
    .exclude(
      "Spark vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - with partition data column - select nested field in Expand")
    .exclude(
      "Non-vectorized reader - without partition data column - select nested field in Expand")
    .exclude("Non-vectorized reader - with partition data column - select nested field in Expand")
    .exclude("Spark vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - without partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Non-vectorized reader - with partition data column - SPARK-32163: nested pruning should work even with cosmetic variations")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38918: nested schema pruning with correlated subqueries")
    .exclude("Case-sensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from array")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34963: extract case-insensitive struct field from struct")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT EXISTS subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated IN subquery")
    .exclude("Spark vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Spark vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - without partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
    .exclude("Non-vectorized reader - with partition data column - SPARK-38977: schema pruning with correlated NOT IN subquery")
  enableSuite[GlutenParquetColumnIndexSuite]
    // Rewrite by just removing test timestamp.
    .exclude("test reading unaligned pages - test all types")
  enableSuite[GlutenParquetCompressionCodecPrecedenceSuite]
  enableSuite[GlutenParquetDeltaByteArrayEncodingSuite]
  enableSuite[GlutenParquetDeltaEncodingInteger]
  enableSuite[GlutenParquetDeltaEncodingLong]
  enableSuite[GlutenParquetDeltaLengthByteArrayEncodingSuite]
  enableSuite[GlutenParquetEncodingSuite]
    // Velox does not support rle encoding, but it can pass when native writer enabled.
    .exclude("parquet v2 pages - rle encoding for boolean value columns")
  enableSuite[GlutenParquetFieldIdIOSuite]
  enableSuite[GlutenParquetFileFormatV1Suite]
  enableSuite[GlutenParquetFileFormatV2Suite]
  enableSuite[GlutenParquetV1FilterSuite]
    // Rewrite.
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
  enableSuite[GlutenParquetV2FilterSuite]
    // Rewrite.
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    // Rewrite for supported INT96 - timestamp.
    .exclude("filter pushdown - timestamp")
    .exclude("filter pushdown - date")
    // Exception bebaviour.
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    // Ignore Spark's filter pushdown check.
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
    .exclude("SPARK-16371 Do not push down filters when inner name and outer name are the same")
    .exclude("filter pushdown - StringPredicate")
  enableSuite[GlutenParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[GlutenParquetIOSuite]
    // Velox doesn't write file metadata into parquet file.
    .exclude("Write Spark version into Parquet metadata")
    // Exception.
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    // Exception msg.
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    // Velox parquet reader not allow offset zero.
    .exclude("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
  enableSuite[GlutenParquetV1QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // new added in spark-3.3 and need fix later, random failure may caused by memory free
    .exclude("SPARK-39833: pushed filters with project without filter columns")
    .exclude("SPARK-39833: pushed filters with count()")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV2QuerySuite]
    .exclude("row group skipping doesn't overflow when reading into larger type")
    // Unsupport spark.sql.files.ignoreCorruptFiles.
    .exclude("Enabling/disabling ignoreCorruptFiles")
    // decimal failed ut
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    // Rewrite because the filter after datasource is not needed.
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
  enableSuite[GlutenParquetV2SchemaPruningSuite]
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ, rewrite some
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetRebaseDatetimeV2Suite]
    // Velox doesn't write file metadata into parquet file.
    .excludeByPrefix("SPARK-33163, SPARK-37705: write the metadata keys")
    .excludeByPrefix("SPARK-33160, SPARK-37705: write the metadata key")
    // jar path and ignore PARQUET_REBASE_MODE_IN_READ
    .excludeByPrefix("SPARK-31159")
    .excludeByPrefix("SPARK-35427")
  enableSuite[GlutenParquetSchemaInferenceSuite]
  enableSuite[GlutenParquetSchemaSuite]
    // error message mismatch is accepted
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
    // [PATH_NOT_FOUND] Path does not exist:
    // file:/opt/spark331/sql/core/src/test/resources/test-data/timestamp-nanos.parquet
    // May require for newer spark.test.home
    .excludeByPrefix("SPARK-40819")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    // Rewrite for file locating.
    .exclude("Read Parquet file generated by parquet-thrift")
  enableSuite[GlutenParquetVectorizedSuite]
  enableSuite[GlutenTextV1Suite]
  enableSuite[GlutenTextV2Suite]
  enableSuite[GlutenDataSourceV2StrategySuite]
  enableSuite[GlutenFileTableSuite]
  enableSuite[GlutenV2PredicateSuite]
  enableSuite[GlutenBucketingUtilsSuite]
  enableSuite[GlutenDataSourceStrategySuite]
  enableSuite[GlutenDataSourceSuite]
  enableSuite[GlutenFileFormatWriterSuite]
    // TODO: fix "empty file should be skipped while write to file"
    .exclude("empty file should be skipped while write to file")
  enableSuite[GlutenFileIndexSuite]
  enableSuite[GlutenFileMetadataStructSuite]
  enableSuite[GlutenParquetV1AggregatePushDownSuite]
  enableSuite[GlutenParquetV2AggregatePushDownSuite]
    // TODO: Timestamp columns stats will lost if using int64 in parquet writer.
    .exclude("aggregate push down - different data types")
  enableSuite[GlutenOrcV1AggregatePushDownSuite]
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenOrcV2AggregatePushDownSuite]
    .exclude("nested column: Max(top level column) not push down")
    .exclude("nested column: Count(nested sub-field) not push down")
  enableSuite[GlutenParquetCodecSuite]
    // codec not supported in native
    .exclude("write and read - file source parquet - codec: lz4_raw")
    .exclude("write and read - file source parquet - codec: lz4raw")
  enableSuite[GlutenOrcCodecSuite]
  enableSuite[GlutenFileSourceStrategySuite]
    // Plan comparison.
    .exclude("partitioned table - after scan filters")
  enableSuite[GlutenHadoopFileLinesReaderSuite]
  enableSuite[GlutenPathFilterStrategySuite]
  enableSuite[GlutenPathFilterSuite]
  enableSuite[GlutenPruneFileSourcePartitionsSuite]
  enableSuite[GlutenCSVReadSchemaSuite]
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
  enableSuite[GlutenJsonReadSchemaSuite]
  enableSuite[GlutenOrcReadSchemaSuite]
    .exclude("append column into middle")
    .exclude("hide column in the middle")
    .exclude("change column position")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("read as string")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
    .exclude("change column type from float to decimal")
    .exclude("change column type from double to decimal")
    .exclude("read float, double, decimal together")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
  enableSuite[GlutenVectorizedOrcReadSchemaSuite]
    // Rewrite to disable Spark's vectorized reading.
    .exclude("change column position")
    .exclude("read byte, int, short, long together")
    .exclude("read float and double together")
    .exclude("append column into middle")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("change column type from float to double")
    .excludeGlutenTest("read byte, int, short, long together")
    .excludeGlutenTest("read float and double together")
  enableSuite[GlutenMergedOrcReadSchemaSuite]
    .exclude("append column into middle")
    .exclude("add a nested column at the end of the leaf struct column")
    .exclude("add a nested column in the middle of the leaf struct column")
    .exclude("add a nested column at the end of the middle struct column")
    .exclude("add a nested column in the middle of the middle struct column")
    .exclude("hide a nested column at the end of the leaf struct column")
    .exclude("hide a nested column in the middle of the leaf struct column")
    .exclude("hide a nested column at the end of the middle struct column")
    .exclude("hide a nested column in the middle of the middle struct column")
    .exclude("change column type from boolean to byte/short/int/long")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
  enableSuite[GlutenParquetReadSchemaSuite]
  enableSuite[GlutenVectorizedParquetReadSchemaSuite]
  enableSuite[GlutenMergedParquetReadSchemaSuite]
  enableSuite[GlutenV1WriteCommandSuite]
    // Rewrite to match SortExecTransformer.
    .excludeByPrefix("SPARK-41914:")
  enableSuite[GlutenEnsureRequirementsSuite]

  enableSuite[GlutenBroadcastJoinSuite]
    .exclude("Shouldn't change broadcast join buildSide if user clearly specified")
    .exclude("Shouldn't bias towards build right if user didn't specify")
    .exclude("SPARK-23192: broadcast hint should be retained after using the cached data")
    .exclude("broadcast join where streamed side's output partitioning is HashPartitioning")

  enableSuite[GlutenExistenceJoinSuite]
  enableSuite[GlutenInnerJoinSuiteForceShjOn]
  enableSuite[GlutenInnerJoinSuiteForceShjOff]
  enableSuite[GlutenOuterJoinSuiteForceShjOn]
  enableSuite[GlutenOuterJoinSuiteForceShjOff]
  enableSuite[FallbackStrategiesSuite]
  enableSuite[GlutenBroadcastExchangeSuite]
  enableSuite[GlutenLocalBroadcastExchangeSuite]
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    .excludeByPrefix("determining the number of reducers")
  enableSuite[GlutenExchangeSuite]
    // ColumnarShuffleExchangeExec does not support doExecute() method
    .exclude("shuffling UnsafeRows in exchange")
    // ColumnarShuffleExchangeExec does not support SORT_BEFORE_REPARTITION
    .exclude("SPARK-23207: Make repartition() generate consistent output")
    // This test will re-run in GlutenExchangeSuite with shuffle partitions > 1
    .exclude("Exchange reuse across the whole plan")
  enableSuite[GlutenReplaceHashWithSortAggSuite]
    .exclude("replace partial hash aggregate with sort aggregate")
    .exclude("replace partial and final hash aggregate together with sort aggregate")
    .exclude("do not replace hash aggregate if child does not have sort order")
    .exclude("do not replace hash aggregate if there is no group-by column")
  enableSuite[GlutenReuseExchangeAndSubquerySuite]
  enableSuite[GlutenSameResultSuite]
  enableSuite[GlutenSortSuite]
  enableSuite[GlutenSQLAggregateFunctionSuite]
  // spill not supported yet.
  enableSuite[GlutenSQLWindowFunctionSuite]
    .exclude("test with low buffer spill threshold")
  enableSuite[GlutenTakeOrderedAndProjectSuite]
  enableSuite[GlutenSessionExtensionSuite]
  enableSuite[TestFileSourceScanExecTransformer]
  enableSuite[GlutenBucketedReadWithoutHiveSupportSuite]
    // Exclude the following suite for plan changed from SMJ to SHJ.
    .exclude("avoid shuffle when join 2 bucketed tables")
    .exclude("avoid shuffle and sort when sort columns are a super set of join keys")
    .exclude("only shuffle one side when join bucketed table and non-bucketed table")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket number")
    .exclude("only shuffle one side when 2 bucketed tables have different bucket keys")
    .exclude("shuffle when join keys are not equal to bucket keys")
    .exclude("shuffle when join 2 bucketed tables with bucketing disabled")
    .exclude("check sort and shuffle when bucket and sort columns are join keys")
    .exclude("only sort one side when sort columns are different")
    .exclude("only sort one side when sort columns are same but their ordering is different")
    .exclude("SPARK-17698 Join predicates should not contain filter clauses")
    .exclude("SPARK-19122 Re-order join predicates if they match with the child's" +
      " output partitioning")
    .exclude("SPARK-19122 No re-ordering should happen if set of join columns != set of child's " +
      "partitioning columns")
    .exclude("SPARK-29655 Read bucketed tables obeys spark.sql.shuffle.partitions")
    .exclude("SPARK-32767 Bucket join should work if SHUFFLE_PARTITIONS larger than bucket number")
    .exclude("bucket coalescing eliminates shuffle")
    .exclude("bucket coalescing is not satisfied")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("disable bucketing when the output doesn't contain all bucketing columns")
    .excludeByPrefix("bucket coalescing is applied when join expressions match")
  enableSuite[GlutenBucketedWriteWithoutHiveSupportSuite]
    .exclude("write bucketed data")
    .exclude("write bucketed data with sortBy")
    .exclude("write bucketed data without partitionBy")
    .exclude("write bucketed data without partitionBy with sortBy")
    .exclude("write bucketed data with bucketing disabled")
  enableSuite[GlutenCreateTableAsSelectSuite]
    // TODO Gluten can not catch the spark exception in Driver side.
    .exclude("CREATE TABLE USING AS SELECT based on the file without write permission")
    .exclude("create a table, drop it and create another one with the same name")
  enableSuite[GlutenDDLSourceLoadSuite]
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuite]
    .disable(
      "DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type")
  enableSuite[GlutenDisableUnnecessaryBucketedScanWithoutHiveSupportSuiteAE]
  enableSuite[GlutenExternalCommandRunnerSuite]
  enableSuite[GlutenFilteredScanSuite]
  enableSuite[GlutenFiltersSuite]
  enableSuite[GlutenInsertSuite]
    // the native write staing dir is differnt with vanilla Spark for coustom partition paths
    .exclude("SPARK-35106: Throw exception when rename custom partition paths returns false")
    .exclude("Stop task set if FileAlreadyExistsException was thrown")
    // Rewrite: Additional support for file scan with default values has been added in Spark-3.4.
    // It appends the default value in record if it is not present while scanning.
    // Velox supports default values for new records but it does not backfill the
    // existing records and provides null for the existing ones.
    .exclude("INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them")
    .exclude("SPARK-39557 INSERT INTO statements with tables with array defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with struct defaults")
    .exclude("SPARK-39557 INSERT INTO statements with tables with map defaults")
  enableSuite[GlutenPartitionedWriteSuite]
  enableSuite[GlutenPathOptionSuite]
  enableSuite[GlutenPrunedScanSuite]
  enableSuite[GlutenResolvedDataSourceSuite]
  enableSuite[GlutenSaveLoadSuite]
  enableSuite[GlutenTableScanSuite]
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
  enableSuite[GlutenApproximatePercentileQuerySuite]
    // requires resource files from Vanilla spark jar
    .exclude("SPARK-32908: maximum target error in percentile_approx")
  enableSuite[GlutenCachedTableSuite]
    .exclude("A cached table preserves the partitioning and ordering of its cached SparkPlan")
    .exclude("InMemoryRelation statistics")
    // Extra ColumnarToRow is needed to transform vanilla columnar data to gluten columnar data.
    .exclude("SPARK-37369: Avoid redundant ColumnarToRow transition on InMemoryTableScan")
  enableSuite[GlutenFileSourceCharVarcharTestSuite]
    .exclude("length check for input string values: nested in array")
    .exclude("length check for input string values: nested in array")
    .exclude("length check for input string values: nested in map key")
    .exclude("length check for input string values: nested in map value")
    .exclude("length check for input string values: nested in both map key and value")
    .exclude("length check for input string values: nested in array of struct")
    .exclude("length check for input string values: nested in array of array")
  enableSuite[GlutenDSV2CharVarcharTestSuite]
  enableSuite[GlutenColumnExpressionSuite]
    // Velox raise_error('errMsg') throws a velox_user_error exception with the message 'errMsg'.
    // The final caught Spark exception's getCause().getMessage() contains 'errMsg' but does not
    // equal 'errMsg' exactly. The following two tests will be skipped and overridden in Gluten.
    .exclude("raise_error")
    .exclude("assert_true")
  enableSuite[GlutenComplexTypeSuite]
  enableSuite[GlutenConfigBehaviorSuite]
    // Will be fixed by cleaning up ColumnarShuffleExchangeExec.
    .exclude("SPARK-22160 spark.sql.execution.rangeExchange.sampleSizePerPartition")
  enableSuite[GlutenCountMinSketchAggQuerySuite]
  enableSuite[GlutenCsvFunctionsSuite]
  enableSuite[GlutenCTEHintSuite]
  enableSuite[GlutenCTEInlineSuiteAEOff]
  enableSuite[GlutenCTEInlineSuiteAEOn]
  enableSuite[GlutenDataFrameAggregateSuite]
    // Test for vanilla spark codegen, not apply for Gluten
    .exclude("SPARK-43876: Enable fast hashmap for distinct queries")
    .exclude(
      "zero moments", // [velox does not return NaN]
      "SPARK-26021: NaN and -0.0 in grouping expressions", // NaN case
      // incorrect result, distinct NaN case
      "SPARK-32038: NormalizeFloatingNumbers should work on distinct aggregate",
      // Replaced with another test.
      "SPARK-19471: AggregationIterator does not initialize the generated result projection" +
        " before using it",
      // Velox's collect_list / collect_set are by design declarative aggregate so plan check
      // for ObjectHashAggregateExec will fail.
      "SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle",
      "SPARK-31620: agg with subquery (whole-stage-codegen = true)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = false)"
    )
  enableSuite[GlutenDataFrameAsOfJoinSuite]
  enableSuite[GlutenDataFrameComplexTypeSuite]
  enableSuite[GlutenDataFrameFunctionsSuite]
    // blocked by Velox-5768
    .exclude("aggregate function - array for primitive type containing null")
    .exclude("aggregate function - array for non-primitive type")
    // Rewrite this test because Velox sorts rows by key for primitive data types, which disrupts the original row sequence.
    .exclude("map_zip_with function - map of primitive types")
  enableSuite[GlutenDataFrameHintSuite]
  enableSuite[GlutenDataFrameImplicitsSuite]
  enableSuite[GlutenDataFrameJoinSuite]
  enableSuite[GlutenDataFrameNaFunctionsSuite]
    .exclude(
      // NaN case
      "replace nan with float",
      "replace nan with double"
    )
  enableSuite[GlutenDataFramePivotSuite]
    // substring issue
    .exclude("pivot with column definition in groupby")
    // array comparison not supported for values that contain nulls
    .exclude(
      "pivot with null and aggregate type not supported by PivotFirst returns correct result")
  enableSuite[GlutenDataFrameRangeSuite]
    .exclude("SPARK-20430 Initialize Range parameters in a driver side")
    .excludeByPrefix("Cancelling stage in a query with Range")
  enableSuite[GlutenDataFrameSelfJoinSuite]
  enableSuite[GlutenDataFrameSessionWindowingSuite]
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("SPARK-37371: UnionExec should support columnar if all children support columnar")
    // Result depends on the implementation for nondeterministic expression rand.
    // Not really an issue.
    .exclude("SPARK-10740: handle nondeterministic expressions correctly for set operations")
  enableSuite[GlutenDataFrameStatSuite]
  enableSuite[GlutenDataFrameSuite]
    // Rewrite these tests because it checks Spark's physical operators.
    .excludeByPrefix("SPARK-22520", "reuse exchange")
    .exclude(
      /**
       * Rewrite these tests because the rdd partition is equal to the configuration
       * "spark.sql.shuffle.partitions".
       */
      "repartitionByRange",
      "distributeBy and localSort",
      // Mismatch when max NaN and infinite value
      "NaN is greater than all other non-NaN numeric values",
      // Rewrite this test because the describe functions creates unmatched plan.
      "describe",
      // decimal failed ut.
      "SPARK-22271: mean overflows and returns null for some decimal variables",
      // Result depends on the implementation for nondeterministic expression rand.
      // Not really an issue.
      "SPARK-9083: sort with non-deterministic expressions"
    )
    // test for sort node not present but gluten uses shuffle hash join
    .exclude("SPARK-41048: Improve output partitioning and ordering with AQE cache")
    // Rewrite this test since it checks the physical operator which is changed in Gluten
    .exclude("SPARK-27439: Explain result should match collected result after view change")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
  enableSuite[GlutenDataFrameTungstenSuite]
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    // does not support `spark.sql.legacy.statisticalAggregate=true` (null -> NAN)
    .exclude("corr, covar_pop, stddev_pop functions in specific window")
    .exclude("covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window")
    // does not support spill
    .exclude("Window spill with more than the inMemoryThreshold and spillThreshold")
    .exclude("SPARK-21258: complex object in combination with spilling")
    // rewrite `WindowExec -> WindowExecTransformer`
    .exclude(
      "SPARK-38237: require all cluster keys for child required distribution for window query")
  enableSuite[GlutenDataFrameWindowFramesSuite]
    // Local window fixes are not added.
    .exclude("range between should accept int/long values as boundary")
    .exclude("unbounded preceding/following range between with aggregation")
    .exclude("sliding range between with aggregation")
    .exclude("store and retrieve column stats in different time zones")
  enableSuite[GlutenDataFrameWriterV2Suite]
  enableSuite[GlutenDatasetAggregatorSuite]
  enableSuite[GlutenDatasetCacheSuite]
  enableSuite[GlutenDatasetOptimizationSuite]
  enableSuite[GlutenDatasetPrimitiveSuite]
  enableSuite[GlutenDatasetSerializerRegistratorSuite]
  enableSuite[GlutenDatasetSuite]
    // Rewrite the following two tests in GlutenDatasetSuite.
    .exclude("dropDuplicates: columns with same column name")
    .exclude("groupBy.as")
    .exclude("dropDuplicates")
    .exclude("select 2, primitive and tuple")
    .exclude("SPARK-16853: select, case class and tuple")
    // TODO: SPARK-16995 may dead loop!!
    .exclude("SPARK-16995: flat mapping on Dataset containing a column created with lit/expr")
    .exclude("SPARK-24762: typed agg on Option[Product] type")
    .exclude("SPARK-40407: repartition should not result in severe data skew")
    .exclude("SPARK-40660: Switch to XORShiftRandom to distribute elements")
  enableSuite[GlutenDateFunctionsSuite]
    // The below two are replaced by two modified versions.
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    // Unsupported datetime format: specifier X is not supported by velox.
    .exclude("to_timestamp with microseconds precision")
    // Legacy mode is not supported, assuming this mode is not commonly used.
    .exclude("SPARK-30668: use legacy timestamp parser in to_timestamp")
    // Legacy mode is not supported and velox getTimestamp function does not throw
    // exception when format is "yyyy-dd-aa".
    .exclude("function to_date")
  enableSuite[GlutenDeprecatedAPISuite]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOff]
    .excludeGlutenTest("Subquery reuse across the whole plan")
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV1SuiteAEOffDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOff]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOn]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOnDisableScan]
  enableSuite[GlutenDynamicPartitionPruningV2SuiteAEOffDisableScan]
  enableSuite[GlutenExpressionsSchemaSuite]
  enableSuite[GlutenExtraStrategiesSuite]
  enableSuite[GlutenFileBasedDataSourceSuite]
    // test data path is jar path, rewrite
    .exclude("Option recursiveFileLookup: disable partition inferring")
    // gluten executor exception cannot get in driver, rewrite
    .exclude("Spark native readers should respect spark.sql.caseSensitive - parquet")
    // shuffle_partitions config is different, rewrite
    .excludeByPrefix("SPARK-22790")
    // plan is different cause metric is different, rewrite
    .excludeByPrefix("SPARK-25237")
    // error msg from velox is different & reader options is not supported, rewrite
    .exclude("Enabling/disabling ignoreMissingFiles using parquet")
    .exclude("Enabling/disabling ignoreMissingFiles using orc")
    .exclude("Spark native readers should respect spark.sql.caseSensitive - orc")
    .exclude("Return correct results when data columns overlap with partition columns")
    .exclude("Return correct results when data columns overlap with partition " +
      "columns (nested data)")
    .exclude("SPARK-31116: Select nested schema with case insensitive mode")
    // exclude as original metric not correct when task offloaded to velox
    .exclude("SPARK-37585: test input metrics for DSV2 with output limits")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support passing data filters to FileScan without partitionFilters")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("File source v2: support partition pruning")
    // DISABLED: GLUTEN-4893 Vanilla UT checks scan operator by exactly matching the class type
    .exclude("SPARK-41017: filter pushdown with nondeterministic predicates")
  enableSuite[GlutenFileScanSuite]
  enableSuite[GlutenGeneratorFunctionSuite]
    .exclude("SPARK-45171: Handle evaluated nondeterministic expression")
  enableSuite[GlutenInjectRuntimeFilterSuite]
    // FIXME: yan
    .exclude("Merge runtime bloom filters")
  enableSuite[GlutenIntervalFunctionsSuite]
  enableSuite[GlutenJoinSuite]
    // exclude as it check spark plan
    .exclude("SPARK-36794: Ignore duplicated key when building relation for semi/anti hash join")
  enableSuite[GlutenMathFunctionsSuite]
  enableSuite[GlutenMetadataCacheSuite]
    .exclude("SPARK-16336,SPARK-27961 Suggest fixing FileNotFoundException")
  enableSuite[GlutenMiscFunctionsSuite]
  enableSuite[GlutenNestedDataSourceV1Suite]
  enableSuite[GlutenNestedDataSourceV2Suite]
  enableSuite[GlutenProcessingTimeSuite]
  enableSuite[GlutenProductAggSuite]
  enableSuite[GlutenReplaceNullWithFalseInPredicateEndToEndSuite]
  enableSuite[GlutenScalaReflectionRelationSuite]
  enableSuite[GlutenSerializationSuite]
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
  enableSuite[GlutenDSV2SQLInsertTestSuite]
  enableSuite[GlutenSQLQuerySuite]
    // Decimal precision exceeds.
    .exclude("should be able to resolve a persistent view")
    // Unstable. Needs to be fixed.
    .exclude("SPARK-36093: RemoveRedundantAliases should not change expression's name")
    // Rewrite from ORC scan to Parquet scan because ORC is not well supported.
    .exclude("SPARK-28156: self-join should not miss cached view")
    .exclude("SPARK-33338: GROUP BY using literal map should not fail")
    // Rewrite to disable plan check for SMJ because SHJ is preferred in Gluten.
    .exclude("SPARK-11111 null-safe join should not use cartesian product")
    // Rewrite to change the information of a caught exception.
    .exclude("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    // Different exception.
    .exclude("run sql directly on files")
    // Not useful and time consuming.
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL")
    .exclude("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class")
    // exception test, rewritten in gluten
    .exclude("the escape character is not allowed to end with")
    // ORC related
    .exclude("SPARK-37965: Spark support read/write orc file with invalid char in field name")
    .exclude("SPARK-38173: Quoted column cannot be recognized correctly when quotedRegexColumnNames is true")
    // Need to support MAP<NullType, NullType>
    .exclude(
      "SPARK-27619: When spark.sql.legacy.allowHashOnMapType is true, hash can be used on Maptype")
  enableSuite[GlutenSQLQueryTestSuite]
  enableSuite[GlutenStatisticsCollectionSuite]
    // The output byte size of Velox is different
    .exclude("SPARK-33687: analyze all tables in a specific database")
  enableSuite[GlutenSubquerySuite]
    .excludeByPrefix(
      "SPARK-26893" // Rewrite this test because it checks Spark's physical operators.
    )
    // exclude as it checks spark plan
    .exclude("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery")
  enableSuite[GlutenTypedImperativeAggregateSuite]
  enableSuite[GlutenUnwrapCastInComparisonEndToEndSuite]
    // Rewrite with NaN test cases excluded.
    .exclude("cases when literal is max")
  enableSuite[GlutenXPathFunctionsSuite]
  enableSuite[GlutenFallbackSuite]
  enableSuite[GlutenHiveSQLQueryCHSuite]
  enableSuite[GlutenCollapseProjectExecTransformerSuite]
  enableSuite[GlutenSparkSessionExtensionSuite]
  enableSuite[GlutenGroupBasedDeleteFromTableSuite]
  enableSuite[GlutenDeltaBasedDeleteFromTableSuite]
  enableSuite[GlutenDataFrameToSchemaSuite]
  enableSuite[GlutenDatasetUnpivotSuite]
  enableSuite[GlutenLateralColumnAliasSuite]
  enableSuite[GlutenParametersSuite]
  enableSuite[GlutenResolveDefaultColumnsSuite]
  enableSuite[GlutenSubqueryHintPropagationSuite]
  enableSuite[GlutenUrlFunctionsSuite]
  enableSuite[GlutenParquetRowIndexSuite]
    .excludeByPrefix("row index generation")
    .excludeByPrefix("invalid row index column type")
  enableSuite[GlutenBitmapExpressionsQuerySuite]
  enableSuite[GlutenEmptyInSuite]
  enableSuite[GlutenRuntimeNullChecksV2Writes]
  enableSuite[GlutenTableOptionsConstantFoldingSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableSuite]
  enableSuite[GlutenDeltaBasedMergeIntoTableUpdateAsDeleteAndInsertSuite]
  enableSuite[GlutenDeltaBasedUpdateAsDeleteAndInsertTableSuite]
    // FIXME: complex type result mismatch
    .exclude("update nested struct fields")
    .exclude("update char/varchar columns")
  enableSuite[GlutenDeltaBasedUpdateTableSuite]
  enableSuite[GlutenGroupBasedMergeIntoTableSuite]
  enableSuite[GlutenFileSourceCustomMetadataStructSuite]
  enableSuite[GlutenParquetFileMetadataStructRowIndexSuite]
  enableSuite[GlutenTableLocationSuite]
  enableSuite[GlutenRemoveRedundantWindowGroupLimitsSuite]

  // ClickHouse excluded tests
  enableSuite[GlutenCSVv1Suite]
    .exclude("simple csv test")
    .exclude("simple csv test with calling another function to load")
    .exclude("simple csv test with type inference")
    .exclude("test with alternative delimiter and quote")
    .exclude("SPARK-24540: test with multiple character delimiter (comma space)")
    .exclude("SPARK-24540: test with multiple (crazy) character delimiter")
    .exclude("test different encoding")
    .exclude("crlf line separators in multiline mode")
    .exclude("test aliases sep and encoding for delimiter and charset")
    .exclude("test for DROPMALFORMED parsing mode")
    .exclude("test for blank column names on read and select columns")
    .exclude("test for FAILFAST parsing mode")
    .exclude("test for tokens more than the fields in the schema")
    .exclude("test with null quote character")
    .exclude("save csv with quote escaping, using charToEscapeQuoteEscaping option")
    .exclude("commented lines in CSV data")
    .exclude("inferring schema with commented lines in CSV data")
    .exclude("inferring timestamp types via custom date format")
    .exclude("load date types via custom date format")
    .exclude("nullable fields with user defined null value of \"null\"")
    .exclude("empty fields with user defined empty values")
    .exclude("old csv data source name works")
    .exclude("nulls, NaNs and Infinity values can be parsed")
    .exclude("SPARK-15585 turn off quotations")
    .exclude("Write timestamps correctly in ISO8601 format by default")
    .exclude("Write dates correctly in ISO8601 format by default")
    .exclude("Roundtrip in reading and writing timestamps")
    .exclude("SPARK-37326: Write and infer TIMESTAMP_LTZ values with a non-default pattern")
    .exclude("SPARK-37326: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-37326: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ")
    .exclude("Write dates correctly with dateFormat option")
    .exclude("Write timestamps correctly with timestampFormat option")
    .exclude("Write timestamps correctly with timestampFormat option and timeZone option")
    .exclude("SPARK-18699 put malformed records in a `columnNameOfCorruptRecord` field")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-19610: Parse normal multi-line CSV files")
    .exclude("SPARK-38523: referring to the corrupt record column")
    .exclude("SPARK-17916: An empty string should not be coerced to null when nullValue is passed.")
    .exclude(
      "SPARK-25241: An empty string should not be coerced to null when emptyValue is passed.")
    .exclude("SPARK-24329: skip lines with comments, and one or multiple whitespaces")
    .exclude("SPARK-23786: Checking column names against schema in the multiline mode")
    .exclude("SPARK-23786: Checking column names against schema in the per-line mode")
    .exclude("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false")
    .exclude("SPARK-23786: warning should be printed if CSV header doesn't conform to schema")
    .exclude("SPARK-25134: check header on parsing of dataset with projection and column pruning")
    .exclude("SPARK-24676 project required data from parsed data when columnPruning disabled")
    .exclude("encoding in multiLine mode")
    .exclude("Support line separator - default value \\r, \\r\\n and \\n")
    .exclude("Support line separator in UTF-8 #0")
    .exclude("Support line separator in UTF-16BE #1")
    .exclude("Support line separator in ISO-8859-1 #2")
    .exclude("Support line separator in UTF-32LE #3")
    .exclude("Support line separator in UTF-8 #4")
    .exclude("Support line separator in UTF-32BE #5")
    .exclude("Support line separator in CP1251 #6")
    .exclude("Support line separator in UTF-16LE #8")
    .exclude("Support line separator in UTF-32BE #9")
    .exclude("Support line separator in US-ASCII #10")
    .exclude("Support line separator in utf-32le #11")
    .exclude("lineSep with 2 chars when multiLine set to true")
    .exclude("lineSep with 2 chars when multiLine set to false")
    .exclude("SPARK-26208: write and read empty data to csv file with headers")
    .exclude("Do not reuse last good value for bad input field")
    .exclude("SPARK-29101 test count with DROPMALFORMED mode")
    .exclude("return correct results when data columns overlap with partition columns")
    .exclude("filters push down - malformed input in PERMISSIVE mode")
    .exclude("case sensitivity of filters references")
    .exclude("SPARK-33566: configure UnescapedQuoteHandling to parse unescaped quotes and unescaped delimiter data correctly")
    .exclude("SPARK-36831: Support reading and writing ANSI intervals")
    .exclude("SPARK-39469: Infer schema for columns with all dates")
    .exclude("SPARK-40474: Infer schema for columns with a mix of dates and timestamp")
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .exclude("SPARK-39731: Handle date and timestamp parsing fallback")
    .exclude("SPARK-40215: enable parsing fallback for CSV in CORRECTED mode with a SQL config")
    .exclude("SPARK-40496: disable parsing fallback when the date/timestamp format is provided")
    .exclude("SPARK-42335: Pass the comment option through to univocity if users set it explicitly in CSV dataSource")
    .exclude("SPARK-46862: column pruning in the multi-line mode")
  enableSuite[GlutenArithmeticExpressionSuite]
    .exclude("% (Remainder)")
    .exclude("SPARK-17617: % (Remainder) double % double on super big double")
    .exclude("pmod")
  enableSuite[GlutenDateFunctionsSuite]
    .exclude("SPARK-30766: date_trunc of old timestamps to hours and days")
    .exclude("SPARK-30793: truncate timestamps before the epoch to seconds and minutes")
    .exclude("try_to_timestamp")
    .exclude("Gluten - to_unix_timestamp")
  enableSuite[GlutenParquetV2FilterSuite]
    .exclude("filter pushdown - StringContains")
    .exclude("SPARK-36866: filter pushdown - year-month interval")
    .exclude("Gluten - filter pushdown - date")
  enableSuite[GlutenParquetV2SchemaPruningSuite]
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
    .exclude("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenSortSuite]
    .exclude("basic sorting using ExternalSort")
    .exclude("SPARK-33260: sort order is a Stream")
    .exclude("SPARK-40089: decimal values sort correctly")
    .exclude(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a ASC NULLS FIRST)")
    .exclude(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a ASC NULLS LAST)")
    .exclude(
      "sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on YearMonthIntervalType(0,1) with nullable=true, sortOrder=List('a DESC NULLS FIRST)")
    .exclude("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a ASC NULLS FIRST)")
    .exclude(
      "sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a ASC NULLS LAST)")
    .exclude("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a DESC NULLS LAST)")
    .exclude("sorting on YearMonthIntervalType(0,1) with nullable=false, sortOrder=List('a DESC NULLS FIRST)")
  enableSuite[GlutenConditionalExpressionSuite]
    .exclude("case when")
  enableSuite[GlutenJsonExpressionsSuite]
    .exclude("from_json - input=object, schema=array, output=array of single row")
    .exclude("from_json - input=empty object, schema=array, output=array of single row with null")
    .exclude("from_json - input=array of single object, schema=struct, output=single row")
    .exclude("from_json - input=array, schema=struct, output=single row")
    .exclude("from_json - input=empty array, schema=struct, output=single row with null")
    .exclude("from_json - input=empty object, schema=struct, output=single row with null")
    .exclude("SPARK-20549: from_json bad UTF-8")
    .exclude("from_json with timestamp")
    .exclude("to_json - struct")
    .exclude("to_json - array")
    .exclude("to_json - array with single empty row")
    .exclude("to_json with timestamp")
    .exclude("SPARK-21513: to_json support map[string, struct] to json")
    .exclude("SPARK-21513: to_json support map[struct, struct] to json")
    .exclude("parse date with locale")
    .exclude("parse decimals using locale")
  enableSuite[GlutenV1WriteCommandSuite]
    .exclude(
      "Gluten - SPARK-41914: v1 write with AQE and in-partition sorted - non-string partition column")
    .exclude(
      "Gluten - SPARK-41914: v1 write with AQE and in-partition sorted - string partition column")
  enableSuite[GlutenInnerJoinSuiteForceShjOn]
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using CartesianProduct")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
  enableSuite[GlutenApproxCountDistinctForIntervalsQuerySuite]
    .exclude("test ApproxCountDistinctForIntervals with large number of endpoints")
  enableSuite[GlutenOrcSourceSuite]
    .exclude(
      "SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=true)")
    .exclude(
      "SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
    .exclude("Gluten - SPARK-31284: compatibility with Spark 2.4 in reading timestamps")
    .exclude("Gluten - SPARK-31284, SPARK-31423: rebasing timestamps in write")
    .exclude(
      "Gluten - SPARK-36931: Support reading and writing ANSI intervals (spark.sql.orc.enableVectorizedReader=false, spark.sql.orc.enableNestedColumnVectorizedReader=false)")
  enableSuite[GlutenCoalesceShufflePartitionsSuite]
    .exclude("SPARK-46590 adaptive query execution works correctly with broadcast join and union")
    .exclude("SPARK-46590 adaptive query execution works correctly with cartesian join and union")
    .exclude("SPARK-24705 adaptive query execution works correctly when exchange reuse enabled")
    .exclude("Do not reduce the number of shuffle partition for repartition")
    .exclude("Union two datasets with different pre-shuffle partition number")
    .exclude("SPARK-34790: enable IO encryption in AQE partition coalescing")
    .exclude("Gluten - determining the number of reducers: aggregate operator(minNumPostShufflePartitions: 5)")
    .exclude(
      "Gluten - determining the number of reducers: join operator(minNumPostShufflePartitions: 5)")
    .exclude(
      "Gluten - determining the number of reducers: complex query 1(minNumPostShufflePartitions: 5)")
    .exclude(
      "Gluten - determining the number of reducers: complex query 2(minNumPostShufflePartitions: 5)")
    .exclude("Gluten - determining the number of reducers: plan already partitioned(minNumPostShufflePartitions: 5)")
    .exclude("Gluten - determining the number of reducers: aggregate operator")
    .exclude("Gluten - determining the number of reducers: join operator")
    .exclude("Gluten - determining the number of reducers: complex query 1")
    .exclude("Gluten - determining the number of reducers: complex query 2")
    .exclude("Gluten - determining the number of reducers: plan already partitioned")
  enableSuite[GlutenDataFramePivotSuite]
    .exclude("SPARK-38133: Grouping by TIMESTAMP_NTZ should not corrupt results")
  enableSuite[GlutenBloomFilterAggregateQuerySuite]
    .exclude("Test bloom_filter_agg and might_contain")
  enableSuite[GlutenDataFrameSessionWindowingSuite]
    .exclude("simple session window with record at window start")
    .exclude("session window groupBy statement")
    .exclude("session window groupBy with multiple keys statement")
    .exclude("session window groupBy with multiple keys statement - two distinct")
    .exclude("session window groupBy with multiple keys statement - keys overlapped with sessions")
    .exclude("SPARK-36465: filter out events with negative/zero gap duration")
    .exclude("SPARK-36724: Support timestamp_ntz as a type of time column for SessionWindow")
  enableSuite[GlutenSubquerySuite]
    .exclude("SPARK-39355: Single column uses quoted to construct UnresolvedAttribute")
    .exclude("SPARK-40800: always inline expressions in OptimizeOneRowRelationSubquery")
    .exclude("SPARK-40862: correlated one-row subquery with non-deterministic expressions")
  enableSuite[GlutenDataSourceV2SQLSuiteV1Filter]
    .exclude("DeleteFrom with v2 filtering: fail if has subquery")
    .exclude("DeleteFrom with v2 filtering: delete with unsupported predicates")
    .exclude("SPARK-33652: DeleteFrom should refresh caches referencing the table")
    .exclude("DeleteFrom: - delete with invalid predicate")
  enableSuite[GlutenExistenceJoinSuite]
    .exclude("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build left")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left semi join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) for left semi join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) for left semi join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastHashJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build left")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("test single unique condition (equal) for left anti join using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed condition (equal & non-equal) test for left anti join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("test composed unique condition (both non-equal) for left anti join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("test composed unique condition (both non-equal) for left anti join using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenColumnExpressionSuite]
    .exclude("withField should add field with no name")
    .exclude("withField should replace all fields with given name in struct")
    .exclude("withField user-facing examples")
    .exclude("dropFields should drop field with no name in struct")
    .exclude("dropFields should drop all fields with given name in struct")
  enableSuite[GlutenJsonV2Suite]
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenGeneratorFunctionSuite]
    .exclude("single explode_outer")
    .exclude("single posexplode_outer")
    .exclude("explode_outer and other columns")
    .exclude("aliased explode_outer")
    .exclude("explode_outer on map")
    .exclude("explode_outer on map with aliases")
    .exclude("SPARK-40963: generator output has correct nullability")
    .exclude("Gluten - SPARK-45171: Handle evaluated nondeterministic expression")
  enableSuite[GlutenHashExpressionsSuite]
    .exclude("sha2")
    .exclude("SPARK-30633: xxHash with different type seeds")
  enableSuite[GlutenDataSourceV2SQLSuiteV2Filter]
    .exclude("DeleteFrom with v2 filtering: fail if has subquery")
    .exclude("DeleteFrom with v2 filtering: delete with unsupported predicates")
    .exclude("SPARK-33652: DeleteFrom should refresh caches referencing the table")
  enableSuite[GlutenSQLQuerySuite]
    .exclude("SPARK-6743: no columns from cache")
    .exclude("external sorting updates peak execution memory")
    .exclude("Struct Star Expansion")
    .exclude("Common subexpression elimination")
    .exclude("SPARK-24940: coalesce and repartition hint")
    .exclude("normalize special floating numbers in subquery")
    .exclude("SPARK-38548: try_sum should return null if overflow happens before merging")
    .exclude("SPARK-38589: try_avg should return null if overflow happens before merging")
    .exclude("Gluten - SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar")
    .exclude("Gluten - the escape character is not allowed to end with")
  enableSuite[GlutenEmptyInSuite]
    .exclude("IN with empty list")
  enableSuite[GlutenParquetV1FilterSuite]
    .exclude("filter pushdown - StringContains")
  enableSuite[GlutenParquetV2PartitionDiscoverySuite]
    .exclude("Various partition value types")
    .exclude("Various inferred partition value types")
    .exclude("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenParquetEncodingSuite]
    .exclude("All Types Dictionary")
    .exclude("All Types Null")
  enableSuite[GlutenMathExpressionsSuite]
    .exclude("tanh")
    .exclude("unhex")
    .exclude("atan2")
    .exclude("SPARK-42045: integer overflow in round/bround")
    .exclude("Gluten - round/bround/floor/ceil")
  enableSuite[GlutenDataFrameWindowFramesSuite]
    .exclude("rows between should accept int/long values as boundary")
    .exclude("reverse preceding/following range between with aggregation")
    .exclude(
      "SPARK-41793: Incorrect result for window frames defined by a range clause on large decimals")
  enableSuite[GlutenLateralColumnAliasSuite]
    .exclude("Lateral alias conflicts with table column - Project")
    .exclude("Lateral alias conflicts with table column - Aggregate")
    .exclude("Lateral alias of a complex type")
    .exclude("Lateral alias reference works with having and order by")
    .exclude("Lateral alias basics - Window on Project")
    .exclude("Lateral alias basics - Window on Aggregate")
  enableSuite[GlutenInnerJoinSuiteForceShjOff]
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, one match per row using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, one match per row using SortMergeJoin (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude(
      "inner join, multiple matches using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, multiple matches using SortMergeJoin (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("inner join, null safe using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen off)")
    .exclude("inner join, null safe using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=left) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using ShuffledHashJoin (build=right) (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using CartesianProduct")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build left (whole-stage-codegen on)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen off)")
    .exclude("SPARK-15822 - test structs as keys using BroadcastNestedLoopJoin build right (whole-stage-codegen on)")
  enableSuite[GlutenCachedTableSuite]
    .exclude("Gluten - InMemoryRelation statistics")
  enableSuite[GlutenCSVv2Suite]
    .exclude("SPARK-36831: Support reading and writing ANSI intervals")
  enableSuite[GlutenBloomFilterAggregateQuerySuiteCGOff]
    .exclude("Test bloom_filter_agg and might_contain")
  enableSuite[GlutenParquetRebaseDatetimeV1Suite]
    .exclude("Gluten - SPARK-31159: rebasing dates in write")
  enableSuite[GlutenParquetThriftCompatibilitySuite]
    .exclude("SPARK-10136 list of primitive list")
  enableSuite[GlutenDataFrameSetOperationsSuite]
    .exclude("union should union DataFrames with UDTs (SPARK-13410)")
    .exclude("SPARK-35756: unionByName support struct having same col names but different sequence")
    .exclude("SPARK-36673: Only merge nullability for Unions of struct")
    .exclude("SPARK-36797: Union should resolve nested columns as top-level columns")
  enableSuite[GlutenStringExpressionsSuite]
    .exclude("StringComparison")
    .exclude("Substring")
    .exclude("string substring_index function")
    .exclude("SPARK-40213: ascii for Latin-1 Supplement characters")
    .exclude("ascii for string")
    .exclude("Mask")
    .exclude("SPARK-42384: Mask with null input")
    .exclude("base64/unbase64 for string")
    .exclude("encode/decode for string")
    .exclude("SPARK-47307: base64 encoding without chunking")
    .exclude("Levenshtein distance threshold")
    .exclude("soundex unit test")
    .exclude("overlay for string")
    .exclude("overlay for byte array")
    .exclude("translate")
    .exclude("FORMAT")
    .exclude("LOCATE")
    .exclude("REPEAT")
    .exclude("ParseUrl")
    .exclude("SPARK-33468: ParseUrl in ANSI mode should fail if input string is not a valid url")
  enableSuite[GlutenOrcV1QuerySuite]
    .exclude(
      "SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
  enableSuite[GlutenParquetV1SchemaPruningSuite]
    .exclude("Case-insensitive parser - mixed-case schema - select with exact column names")
    .exclude("Case-insensitive parser - mixed-case schema - select with lowercase column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - select with different-case column names")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("Case-insensitive parser - mixed-case schema - subquery filter with different-case column names")
    .exclude("SPARK-36352: Spark should check result plan's output schema name")
    .exclude("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenDataFrameToSchemaSuite]
    .exclude("struct value: compatible field nullability")
    .exclude("map value: reorder inner fields by name")
  enableSuite[errors.GlutenQueryExecutionErrorsSuite]
    .exclude("CONVERSION_INVALID_INPUT: to_binary conversion function base64")
    .exclude("UNSUPPORTED_FEATURE - SPARK-38504: can't read TimestampNTZ as TimestampLTZ")
    .exclude("CANNOT_PARSE_DECIMAL: unparseable decimal")
    .exclude("UNRECOGNIZED_SQL_TYPE: unrecognized SQL type DATALINK")
    .exclude("UNSUPPORTED_FEATURE.MULTI_ACTION_ALTER: The target JDBC server hosting table does not support ALTER TABLE with multiple actions.")
    .exclude("INVALID_BITMAP_POSITION: position out of bounds")
    .exclude("INVALID_BITMAP_POSITION: negative position")
  enableSuite[GlutenLocalBroadcastExchangeSuite]
    .exclude("SPARK-39983 - Broadcasted relation is not cached on the driver")
  enableSuite[GlutenOuterJoinSuiteForceShjOff]
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("SPARK-32717: AQEOptimizer should respect excludedRules configuration")
  enableSuite[GlutenRemoveRedundantWindowGroupLimitsSuite]
    .exclude("remove redundant WindowGroupLimits")
  enableSuite[GlutenOrcV2SchemaPruningSuite]
    .exclude("Spark vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - without partition data column - select a single complex field and in where clause")
    .exclude("Non-vectorized reader - with partition data column - select a single complex field and in where clause")
    .exclude("Spark vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - without partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Non-vectorized reader - with partition data column - select one complex field and having is null predicate on another complex field")
    .exclude("Spark vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - without partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Non-vectorized reader - with partition data column - SPARK-34638: nested column prune on generator output - case-sensitivity")
    .exclude("Spark vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Spark vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - without partition data column - select one deep nested complex field after repartition by expression")
    .exclude("Non-vectorized reader - with partition data column - select one deep nested complex field after repartition by expression")
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude("SPARK-37450: Prunes unnecessary fields from Explode for count aggregation")
    .exclude("Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude("Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenParquetRowIndexSuite]
    .exclude("Gluten - row index generation - vectorized reader, small pages, small row groups, small splits, datasource v2")
    .exclude("Gluten - row index generation - vectorized reader, small pages, small row groups, datasource v2")
    .exclude("Gluten - row index generation - vectorized reader, small row groups, small splits, datasource v2")
    .exclude("Gluten - row index generation - vectorized reader, small row groups, datasource v2")
    .exclude("Gluten - row index generation - vectorized reader, small pages, datasource v2")
    .exclude("Gluten - row index generation - vectorized reader, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, small pages, small row groups, small splits, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, small pages, small row groups, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, small row groups, small splits, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, small row groups, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, small pages, datasource v2")
    .exclude("Gluten - row index generation - parquet-mr reader, datasource v2")
  enableSuite[errors.GlutenQueryCompilationErrorsSuite]
    .exclude("CREATE NAMESPACE with LOCATION for JDBC catalog should throw an error")
    .exclude(
      "ALTER NAMESPACE with property other than COMMENT for JDBC catalog should throw an exception")
  enableSuite[GlutenMetadataColumnSuite]
    .exclude("SPARK-34923: propagate metadata columns through Sort")
    .exclude("SPARK-34923: propagate metadata columns through RepartitionBy")
    .exclude("SPARK-40149: select outer join metadata columns with DataFrame API")
    .exclude("SPARK-42683: Project a metadata column by its logical name - column not found")
  enableSuite[GlutenTryEvalSuite]
    .exclude("try_subtract")
  enableSuite[GlutenParquetColumnIndexSuite]
    .exclude("test reading unaligned pages - test all types (dict encode)")
  enableSuite[GlutenOrcV2QuerySuite]
    .exclude(
      "SPARK-37728: Reading nested columns with ORC vectorized reader should not cause ArrayIndexOutOfBoundsException")
  enableSuite[GlutenFileSourceSQLInsertTestSuite]
    .exclude("SPARK-33474: Support typed literals as partition spec values")
    .exclude(
      "SPARK-34556: checking duplicate static partition columns should respect case sensitive conf")
  enableSuite[GlutenOrcV1SchemaPruningSuite]
    .exclude(
      "Case-insensitive parser - mixed-case schema - filter with different-case column names")
    .exclude(
      "Spark vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude(
      "Spark vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude(
      "Non-vectorized reader - without partition data column - SPARK-40033: Schema pruning support through element_at")
    .exclude(
      "Non-vectorized reader - with partition data column - SPARK-40033: Schema pruning support through element_at")
  enableSuite[GlutenCSVLegacyTimeParserSuite]
    .exclude("simple csv test")
    .exclude("simple csv test with calling another function to load")
    .exclude("simple csv test with type inference")
    .exclude("test with alternative delimiter and quote")
    .exclude("SPARK-24540: test with multiple character delimiter (comma space)")
    .exclude("SPARK-24540: test with multiple (crazy) character delimiter")
    .exclude("test different encoding")
    .exclude("crlf line separators in multiline mode")
    .exclude("test aliases sep and encoding for delimiter and charset")
    .exclude("test for DROPMALFORMED parsing mode")
    .exclude("test for blank column names on read and select columns")
    .exclude("test for FAILFAST parsing mode")
    .exclude("test for tokens more than the fields in the schema")
    .exclude("test with null quote character")
    .exclude("save csv with quote escaping, using charToEscapeQuoteEscaping option")
    .exclude("commented lines in CSV data")
    .exclude("inferring schema with commented lines in CSV data")
    .exclude("inferring timestamp types via custom date format")
    .exclude("load date types via custom date format")
    .exclude("nullable fields with user defined null value of \"null\"")
    .exclude("empty fields with user defined empty values")
    .exclude("old csv data source name works")
    .exclude("nulls, NaNs and Infinity values can be parsed")
    .exclude("SPARK-15585 turn off quotations")
    .exclude("Write timestamps correctly in ISO8601 format by default")
    .exclude("Write dates correctly in ISO8601 format by default")
    .exclude("Roundtrip in reading and writing timestamps")
    .exclude("SPARK-37326: Write and infer TIMESTAMP_LTZ values with a non-default pattern")
    .exclude("SPARK-37326: Timestamp type inference for a mix of TIMESTAMP_NTZ and TIMESTAMP_LTZ")
    .exclude("Write dates correctly with dateFormat option")
    .exclude("Write timestamps correctly with timestampFormat option")
    .exclude("Write timestamps correctly with timestampFormat option and timeZone option")
    .exclude("SPARK-18699 put malformed records in a `columnNameOfCorruptRecord` field")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude("SPARK-19610: Parse normal multi-line CSV files")
    .exclude("SPARK-38523: referring to the corrupt record column")
    .exclude("SPARK-17916: An empty string should not be coerced to null when nullValue is passed.")
    .exclude(
      "SPARK-25241: An empty string should not be coerced to null when emptyValue is passed.")
    .exclude("SPARK-24329: skip lines with comments, and one or multiple whitespaces")
    .exclude("SPARK-23786: Checking column names against schema in the multiline mode")
    .exclude("SPARK-23786: Checking column names against schema in the per-line mode")
    .exclude("SPARK-23786: Ignore column name case if spark.sql.caseSensitive is false")
    .exclude("SPARK-23786: warning should be printed if CSV header doesn't conform to schema")
    .exclude("SPARK-25134: check header on parsing of dataset with projection and column pruning")
    .exclude("SPARK-24676 project required data from parsed data when columnPruning disabled")
    .exclude("encoding in multiLine mode")
    .exclude("Support line separator - default value \\r, \\r\\n and \\n")
    .exclude("Support line separator in UTF-8 #0")
    .exclude("Support line separator in UTF-16BE #1")
    .exclude("Support line separator in ISO-8859-1 #2")
    .exclude("Support line separator in UTF-32LE #3")
    .exclude("Support line separator in UTF-8 #4")
    .exclude("Support line separator in UTF-32BE #5")
    .exclude("Support line separator in CP1251 #6")
    .exclude("Support line separator in UTF-16LE #8")
    .exclude("Support line separator in UTF-32BE #9")
    .exclude("Support line separator in US-ASCII #10")
    .exclude("Support line separator in utf-32le #11")
    .exclude("lineSep with 2 chars when multiLine set to true")
    .exclude("lineSep with 2 chars when multiLine set to false")
    .exclude("SPARK-26208: write and read empty data to csv file with headers")
    .exclude("Do not reuse last good value for bad input field")
    .exclude("SPARK-29101 test count with DROPMALFORMED mode")
    .exclude("return correct results when data columns overlap with partition columns")
    .exclude("filters push down - malformed input in PERMISSIVE mode")
    .exclude("case sensitivity of filters references")
    .exclude("SPARK-33566: configure UnescapedQuoteHandling to parse unescaped quotes and unescaped delimiter data correctly")
    .exclude("SPARK-36831: Support reading and writing ANSI intervals")
    .exclude("SPARK-39731: Correctly parse dates and timestamps with yyyyMMdd pattern")
    .exclude("SPARK-39731: Handle date and timestamp parsing fallback")
    .exclude("SPARK-40215: enable parsing fallback for CSV in CORRECTED mode with a SQL config")
    .exclude("SPARK-40496: disable parsing fallback when the date/timestamp format is provided")
    .exclude("SPARK-42335: Pass the comment option through to univocity if users set it explicitly in CSV dataSource")
    .exclude("SPARK-46862: column pruning in the multi-line mode")
  enableSuite[GlutenDataFrameTimeWindowingSuite]
    .exclude("simple tumbling window with record at window start")
    .exclude("SPARK-21590: tumbling window using negative start time")
    .exclude("tumbling window groupBy statement")
    .exclude("tumbling window groupBy statement with startTime")
    .exclude("SPARK-21590: tumbling window groupBy statement with negative startTime")
    .exclude("sliding window grouping")
    .exclude("time window joins")
    .exclude("millisecond precision sliding windows")
  enableSuite[GlutenJsonLegacyTimeParserSuite]
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenInsertSuite]
    .exclude("Gluten - insert partition table")
    .exclude("Gluten - remove v1writes sort and project")
    .exclude("Gluten - remove v1writes sort")
    .exclude("Gluten - do not remove non-v1writes sort and project")
    .exclude(
      "Gluten - SPARK-35106: Throw exception when rename custom partition paths returns false")
    .exclude(
      "Gluten - Do not fallback write files if output columns contain Spark internal metadata")
    .exclude("Gluten - Add metadata white list to allow native write files")
    .exclude("Gluten - INSERT rows, ALTER TABLE ADD COLUMNS with DEFAULTs, then SELECT them")
  enableSuite[GlutenRegexpExpressionsSuite]
    .exclude("LIKE Pattern")
    .exclude("LIKE Pattern ESCAPE '/'")
    .exclude("LIKE Pattern ESCAPE '#'")
    .exclude("LIKE Pattern ESCAPE '\"'")
    .exclude("RLIKE Regular Expression")
    .exclude("RegexReplace")
    .exclude("RegexExtract")
    .exclude("RegexExtractAll")
    .exclude("SPLIT")
  enableSuite[GlutenFileBasedDataSourceSuite]
    .exclude("SPARK-23072 Write and read back unicode column names - csv")
    .exclude("Enabling/disabling ignoreMissingFiles using csv")
    .exclude("SPARK-30362: test input metrics for DSV2")
    .exclude("SPARK-35669: special char in CSV header with filter pushdown")
    .exclude("Gluten - Spark native readers should respect spark.sql.caseSensitive - parquet")
    .exclude("Gluten - SPARK-25237 compute correct input metrics in FileScanRDD")
    .exclude("Gluten - Enabling/disabling ignoreMissingFiles using orc")
    .exclude("Gluten - Enabling/disabling ignoreMissingFiles using parquet")
  enableSuite[GlutenDataFrameStatSuite]
    .exclude("SPARK-30532 stat functions to understand fully-qualified column name")
    .exclude("special crosstab elements (., '', null, ``)")
  enableSuite[GlutenDataSourceV2FunctionSuite]
    .exclude("view should use captured catalog and namespace for function lookup")
    .exclude("aggregate function: lookup int average")
    .exclude("aggregate function: lookup long average")
    .exclude("aggregate function: lookup double average in Java")
    .exclude("aggregate function: lookup int average w/ expression")
    .exclude("SPARK-35390: aggregate function w/ type coercion")
  enableSuite[GlutenCollapseProjectExecTransformerSuite]
    .exclude("Gluten - Support ProjectExecTransformer collapse")
  enableSuite[GlutenJsonV1Suite]
    .exclude("SPARK-37360: Write and infer TIMESTAMP_NTZ values with a non-default pattern")
    .exclude("SPARK-37360: Timestamp type inference for a column with TIMESTAMP_NTZ values")
    .exclude("SPARK-36830: Support reading and writing ANSI intervals")
  enableSuite[GlutenPredicateSuite]
    .exclude("basic IN/INSET predicate test")
    .exclude("IN with different types")
    .exclude("IN/INSET: binary")
    .exclude("IN/INSET: struct")
    .exclude("IN/INSET: array")
    .exclude("BinaryComparison: lessThan")
    .exclude("BinaryComparison: LessThanOrEqual")
    .exclude("BinaryComparison: GreaterThan")
    .exclude("BinaryComparison: GreaterThanOrEqual")
    .exclude("EqualTo on complex type")
    .exclude("SPARK-32764: compare special double/float values")
    .exclude("SPARK-32110: compare special double/float values in struct")
  enableSuite[GlutenParquetProtobufCompatibilitySuite]
    .exclude("struct with unannotated array")
  enableSuite[GlutenTakeOrderedAndProjectSuite]
    .exclude("TakeOrderedAndProject.doExecute without project")
    .exclude("TakeOrderedAndProject.doExecute with project")
    .exclude("TakeOrderedAndProject.doExecute with local sort")
  enableSuite[GlutenHeaderCSVReadSchemaSuite]
    .exclude("append column at the end")
    .exclude("hide column at the end")
    .exclude("change column type from byte to short/int/long")
    .exclude("change column type from short to int/long")
    .exclude("change column type from int to long")
    .exclude("read byte, int, short, long together")
    .exclude("change column type from float to double")
    .exclude("read float and double together")
    .exclude("change column type from float to decimal")
    .exclude("change column type from double to decimal")
    .exclude("read float, double, decimal together")
    .exclude("read as string")
  enableSuite[gluten.GlutenFallbackSuite]
    .exclude("Gluten - test fallback event")
  enableSuite[GlutenJoinSuite]
    .exclude(
      "SPARK-45882: BroadcastHashJoinExec propagate partitioning should respect CoalescedHashPartitioning")
  enableSuite[GlutenDataFrameFunctionsSuite]
    .exclude("map with arrays")
    .exclude("flatten function")
    .exclude("SPARK-41233: array prepend")
    .exclude("array_insert functions")
    .exclude("aggregate function - array for primitive type not containing null")
    .exclude("transform keys function - primitive data types")
    .exclude("transform values function - test primitive data types")
    .exclude("transform values function - test empty")
    .exclude("SPARK-14393: values generated by non-deterministic functions shouldn't change after coalesce or union")
    .exclude("mask function")
  enableSuite[GlutenCollectionExpressionsSuite]
    .exclude("Sequence of numbers")
    .exclude("Array Insert")
    .exclude("SPARK-36753: ArrayExcept should handle duplicated Double.NaN and Float.Nan")
    .exclude(
      "SPARK-36740: ArrayMin/ArrayMax/SortArray should handle NaN greater than non-NaN value")
    .exclude("SPARK-42401: Array insert of null value (explicit)")
    .exclude("SPARK-42401: Array insert of null value (implicit)")
  enableSuite[GlutenParquetV1PartitionDiscoverySuite]
    .exclude("Various partition value types")
    .exclude("Various inferred partition value types")
    .exclude("Resolve type conflicts - decimals, dates and timestamps in partition column")
  enableSuite[GlutenCastSuite]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("Gluten - data type casting")
  enableSuite[GlutenKeyGroupedPartitioningSuite]
    .exclude("Gluten - partitioned join: only one side reports partitioning")
    .exclude("Gluten - SPARK-41413: partitioned join: partition values from one side are subset of those from the other side")
    .exclude("Gluten - SPARK-41413: partitioned join: partition values from both sides overlaps")
    .exclude(
      "Gluten - SPARK-41413: partitioned join: non-overlapping partition values from both sides")
    .exclude("Gluten - SPARK-42038: partially clustered: with different partition keys and both sides partially clustered")
    .exclude("Gluten - SPARK-42038: partially clustered: with different partition keys and missing keys on left-hand side")
    .exclude("Gluten - SPARK-42038: partially clustered: with different partition keys and missing keys on right-hand side")
    .exclude("Gluten - SPARK-42038: partially clustered: left outer join")
    .exclude("Gluten - SPARK-42038: partially clustered: right outer join")
    .exclude("Gluten - SPARK-42038: partially clustered: full outer join is not applicable")
    .exclude("Gluten - SPARK-44641: duplicated records when SPJ is not triggered")
    .exclude(
      "Gluten - partitioned join: join with two partition keys and different # of partition keys")
  enableSuite[GlutenDataFrameJoinSuite]
    .exclude("SPARK-32693: Compare two dataframes with same schema except nullable property")
  enableSuite[GlutenTryCastSuite]
    .exclude("null cast")
    .exclude("cast string to date")
    .exclude("cast string to timestamp")
    .exclude("SPARK-22825 Cast array to string")
    .exclude("SPARK-33291: Cast array with null elements to string")
    .exclude("SPARK-22973 Cast map to string")
    .exclude("SPARK-22981 Cast struct to string")
    .exclude("SPARK-33291: Cast struct with null elements to string")
    .exclude("SPARK-35111: Cast string to year-month interval")
    .exclude("cast from timestamp II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z II")
    .exclude("cast a timestamp before the epoch 1970-01-01 00:00:00Z")
    .exclude("cast from array II")
    .exclude("cast from array III")
    .exclude("cast from struct III")
    .exclude("ANSI mode: cast string to timestamp with parse error")
    .exclude("ANSI mode: cast string to date with parse error")
    .exclude("Gluten - data type casting")
  enableSuite[GlutenPartitionedWriteSuite]
    .exclude("SPARK-37231, SPARK-37240: Dynamic writes/reads of ANSI interval partitions")
  enableSuite[GlutenHigherOrderFunctionsSuite]
    .exclude("ArraySort")
    .exclude("ArrayAggregate")
    .exclude("TransformKeys")
    .exclude("TransformValues")
    .exclude("SPARK-39419: ArraySort should throw an exception when the comparator returns null")
  enableSuite[GlutenJsonFunctionsSuite]
    .exclude("from_json with option (allowComments)")
    .exclude("from_json with option (allowUnquotedFieldNames)")
    .exclude("from_json with option (allowSingleQuotes)")
    .exclude("from_json with option (allowNumericLeadingZeros)")
    .exclude("from_json with option (allowBackslashEscapingAnyCharacter)")
    .exclude("from_json with option (dateFormat)")
    .exclude("from_json with option (allowUnquotedControlChars)")
    .exclude("from_json with option (allowNonNumericNumbers)")
    .exclude("from_json missing columns")
    .exclude("from_json invalid json")
    .exclude("from_json array support")
    .exclude("to_json with option (timestampFormat)")
    .exclude("to_json with option (dateFormat)")
    .exclude("SPARK-19637 Support to_json in SQL")
    .exclude("pretty print - roundtrip from_json -> to_json")
    .exclude("from_json invalid json - check modes")
    .exclude("SPARK-36069: from_json invalid json schema - check field name and field value")
    .exclude("corrupt record column in the middle")
    .exclude("parse timestamps with locale")
    .exclude("SPARK-33134: return partial results only for root JSON objects")
    .exclude("SPARK-40646: return partial results for JSON arrays with objects")
    .exclude("SPARK-40646: return partial results for JSON maps")
    .exclude("SPARK-40646: return partial results for objects with values as JSON arrays")
    .exclude("SPARK-48863: parse object as an array with partial results enabled")
    .exclude("SPARK-33907: bad json input with json pruning optimization: GetStructField")
    .exclude("SPARK-33907: bad json input with json pruning optimization: GetArrayStructFields")
    .exclude("SPARK-33907: json pruning optimization with corrupt record field")
  enableSuite[GlutenParquetFileFormatV1Suite]
    .exclude(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenDataFrameSuite]
    .exclude("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow")
    .exclude("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow")
    .exclude("summary")
    .exclude(
      "SPARK-8608: call `show` on local DataFrame with random columns should return same value")
    .exclude("SPARK-8609: local DataFrame with random columns should return same value after sort")
    .exclude("SPARK-10316: respect non-deterministic expressions in PhysicalOperation")
    .exclude("Uuid expressions should produce same results at retries in the same DataFrame")
    .exclude("Gluten - repartitionByRange")
    .exclude("Gluten - describe")
    .exclude("Gluten - Allow leading/trailing whitespace in string before casting")
  enableSuite[GlutenDataFrameWindowFunctionsSuite]
    .exclude(
      "SPARK-13860: corr, covar_pop, stddev_pop functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .exclude(
      "SPARK-13860: covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window LEGACY_STATISTICAL_AGGREGATE off")
    .exclude("lead/lag with ignoreNulls")
    .exclude("SPARK-37099: Insert window group limit node for top-k computation")
    .exclude("Gluten - corr, covar_pop, stddev_pop functions in specific window")
  enableSuite[GlutenParquetFileMetadataStructRowIndexSuite]
    .exclude("reading _tmp_metadata_row_index - present in a table")
  enableSuite[GlutenFileSourceStrategySuite]
    .exclude("unpartitioned table, single partition")
    .exclude("SPARK-32019: Add spark.sql.files.minPartitionNum config")
    .exclude(
      "SPARK-32352: Partially push down support data filter if it mixed in partition filters")
    .exclude("SPARK-44021: Test spark.sql.files.maxPartitionNum works as expected")
  enableSuite[GlutenSQLWindowFunctionSuite]
    .exclude(
      "window function: multiple window expressions specified by range in a single expression")
    .exclude("Gluten - Filter on row number")
  enableSuite[GlutenUrlFunctionsSuite]
    .exclude("url parse_url function")
    .exclude("url encode/decode function")
  enableSuite[GlutenStringFunctionsSuite]
    .exclude("string Levenshtein distance")
    .exclude("string regexp_count")
    .exclude("string regex_replace / regex_extract")
    .exclude("string regexp_extract_all")
    .exclude("string regexp_substr")
    .exclude("string overlay function")
    .exclude("binary overlay function")
    .exclude("string / binary length function")
    .exclude("SPARK-36751: add octet length api for scala")
    .exclude("SPARK-36751: add bit length api for scala")
    .exclude("str_to_map function")
    .exclude("SPARK-42384: mask with null input")
    .exclude("like & ilike function")
    .exclude("parse_url")
    .exclude("url_decode")
    .exclude("url_encode")
  enableSuite[GlutenOuterJoinSuiteForceShjOn]
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic left outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic right outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen off)")
    .exclude("basic full outer join using SortMergeJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("left outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("right outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using ShuffledHashJoin (whole-stage-codegen on)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen off)")
    .exclude("full outer join with unique keys using SortMergeJoin (whole-stage-codegen on)")
  enableSuite[GlutenStatisticsCollectionSuite]
    .exclude("analyze empty table")
    .exclude("analyze column command - result verification")
    .exclude("column stats collection for null columns")
    .exclude("store and retrieve column stats in different time zones")
    .exclude("SPARK-42777: describe column stats (min, max) for timestamp_ntz column")
    .exclude("Gluten - store and retrieve column stats in different time zones")
  enableSuite[GlutenDataFrameAggregateSuite]
    .exclude("linear regression")
    .exclude("collect functions")
    .exclude("collect functions structs")
    .exclude("SPARK-17641: collect functions should not collect null values")
    .exclude("collect functions should be able to cast to array type with no null values")
    .exclude("SPARK-45599: Neither 0.0 nor -0.0 should be dropped when computing percentile")
    .exclude("SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`")
    .exclude("SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`")
    .exclude("SPARK-35412: groupBy of year-month/day-time intervals should work")
    .exclude("SPARK-36054: Support group by TimestampNTZ column")
  enableSuite[GlutenParquetFileFormatV2Suite]
    .exclude(
      "SPARK-36825, SPARK-36854: year-month/day-time intervals written and read as INT32/INT64")
  enableSuite[GlutenDateExpressionsSuite]
    .exclude("DayOfYear")
    .exclude("Quarter")
    .exclude("Month")
    .exclude("Day / DayOfMonth")
    .exclude("DayOfWeek")
    .exclude("WeekDay")
    .exclude("WeekOfYear")
    .exclude("add_months")
    .exclude("months_between")
    .exclude("TruncDate")
    .exclude("unsupported fmt fields for trunc/date_trunc results null")
    .exclude("to_utc_timestamp")
    .exclude("from_utc_timestamp")
    .exclude("SPARK-31896: Handle am-pm timestamp parsing when hour is missing")
    .exclude("UNIX_SECONDS")
    .exclude("TIMESTAMP_SECONDS")

  override def getSQLQueryTestSettings: SQLQueryTestSettings = ClickHouseSQLQueryTestSettings
}
// scalastyle:on line.size.limit
