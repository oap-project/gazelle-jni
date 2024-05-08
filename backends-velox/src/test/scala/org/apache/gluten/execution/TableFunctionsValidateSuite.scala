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

class TableFunctionsValidateSuite extends FunctionsValidateTest {
  disableFallbackCheck

  test("Test json_tuple function") {
    runQueryAndCompare(
      "SELECT * from datatab lateral view json_tuple(string_field1, 'a', 'b') as fa, fb") {
      checkGlutenOperatorMatch[GenerateExecTransformer]
    }

    runQueryAndCompare("SELECT l_orderkey, " +
      "json_tuple('{\"a\" : 1, \"b\" : 2}', CAST(NULL AS STRING), 'b', CAST(NULL AS STRING), 'a') " +
      "from lineitem") {
      checkGlutenOperatorMatch[GenerateExecTransformer]
    }
  }
}
