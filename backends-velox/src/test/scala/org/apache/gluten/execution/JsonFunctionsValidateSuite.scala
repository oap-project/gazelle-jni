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
package org.apache.gluten.execution

import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.ProjectExec

class JsonFunctionsValidateSuite extends FunctionsValidateSuite {

  disableFallbackCheck
  import testImplicits._

  test("get_json_object") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.a') " +
        "from datatab limit 1;") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    runQueryAndCompare(
      "SELECT l_orderkey, get_json_object('{\"a\":\"b\"}', '$.a') " +
        "from lineitem limit 1;") {
      checkGlutenOperatorMatch[ProjectExecTransformer]
    }

    // Invalid UTF-8 encoding.
    spark.sql(
      "CREATE TABLE t USING parquet SELECT concat('{\"a\": 2, \"'," +
        " string(X'80'), '\": 3, \"c\": 100}') AS c1")
    withTable("t") {
      runQueryAndCompare("SELECT get_json_object(c1, '$.c') FROM t;") {
        checkGlutenOperatorMatch[ProjectExecTransformer]
      }
    }
  }

  ignore("json_array_length") {
    runQueryAndCompare(
      s"select *, json_array_length(string_field1) " +
        s"from datatab limit 5")(checkGlutenOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, json_array_length('[1,2,3,4]') " +
        s"from lineitem limit 5")(checkGlutenOperatorMatch[ProjectExecTransformer])
    runQueryAndCompare(
      s"select l_orderkey, json_array_length(null) " +
        s"from lineitem limit 5")(checkGlutenOperatorMatch[ProjectExecTransformer])
  }

  test("from_json function bool") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":0}"""),
            ("""{"id":0.0}"""),
            ("""{"id":true}"""),
            ("""{"id":false}"""),
            ("""{"id":"true"}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id BOOLEAN') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function small int") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":100000000}"""),
            ("""{"id":11.0}"""),
            ("""{"id":'true'}"""),
            ("""{"id":true}"""),
            ("""{"id":'12'}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id SHORT') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function int") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":10}"""),
            ("""{"id":11.0}"""),
            ("""{"id":"true"}"""),
            ("""{"id":true}"""),
            ("""{"id":"12"}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id INT') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function big int") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":100000000}"""),
            ("""{"id":11.0}"""),
            ("""{"id":'true'}"""),
            ("""{"id":true}"""),
            ("""{"id":'12'}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id LONG') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function float") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":10}"""),
            ("""{"id":11.0}"""),
            ("""{"id":"12.0"}"""),
            ("""{"id":"test"}"""),
            ("""{"id":"12"}"""),
            ("""{"id":"-INF"}"""),
            ("""{"id":"NaN"}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id FLOAT') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function double") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":10}"""),
            ("""{"id":11.0}"""),
            ("""{"id":"12.0"}"""),
            ("""{"id":"test"}"""),
            ("""{"id":"12"}"""),
            ("""{"id":"+INF"}"""),
            ("""{"id":"NaN"}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id DOUBLE') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function string") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":10}"""),
            ("""{"id":false}"""),
            ("""{"id":"00010"}"""),
            ("""{"id":[1,2]}"""),
            ("""{"id":{"a":1}}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id STRING') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function array") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""[]"""),
            ("""[1, 3]"""),
            ("""[1, 2, 3.0]""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select from_json(txt, 'array<int>') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function map") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":10, "value":11}"""),
            ("""{"id":11, "value":11.0}"""),
            ("""{"id":10, "Id":11}"""),
            ("""{4:10, "Id":11}"""),
            ("""{}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select from_json(txt, 'map<string,int>') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function row") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"Id":"10", "Value":"11"}"""),
            ("""{"Id":"11", "Value":"11.0"}"""),
            ("""{"Id":"10", "Value":"11"}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select from_json(txt, 'Id STRING, Value STRING') from tbl") {
            checkGlutenOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }

  test("from_json function CORRUPT_RECORD") {
    withTempPath {
      path =>
        Seq[(String)](
          ("""{"id":00010}"""),
          ("""{"id":1.0}""")
        )
          .toDF("txt")
          .write
          .parquet(path.getCanonicalPath)

        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

        runQueryAndCompare(
          "select txt, from_json(txt, 'id INT, _corrupt_record STRING') from tbl") {
          checkSparkOperatorMatch[ProjectExec]
        }
    }
  }

  test("from_json function duplicate key") {
    if (SparkShimLoader.getSparkShims.fromJsonSupportPartialResults) {
      withTempPath {
        path =>
          Seq[(String)](
            ("""{"id":1,"Id":2}"""),
            ("""{"id":3,"Id":4}""")
          )
            .toDF("txt")
            .write
            .parquet(path.getCanonicalPath)

          spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")

          runQueryAndCompare("select txt, from_json(txt, 'id INT, Id INT') from tbl") {
            checkSparkOperatorMatch[ProjectExec]
          }

          runQueryAndCompare("select txt, from_json(txt, 'id INT, id INT') from tbl") {
            checkSparkOperatorMatch[ProjectExec]
          }

          runQueryAndCompare("select txt, from_json(txt, 'id INT') from tbl") {
            checkSparkOperatorMatch[ProjectExecTransformer]
          }
      }
    }
  }
}
