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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.GlutenTestsTrait
import org.apache.spark.sql.types.IntegerType

class GlutenPredicateSuite extends PredicateSuite with GlutenTestsTrait {

  private val leftValues = Seq(1, null, null, 3, 5).map(Literal(_))
  private val rightValues = Seq(null, 2, null, 3, 6).map(Literal(_))
  private val expected = Seq(false, false, true, true, false)

  testGluten("EqualNullSafe") {
    for (i <- leftValues.indices) {
      checkEvaluation(
        EqualNullSafe(Cast(leftValues(i), IntegerType), Cast(rightValues(i), IntegerType)),
        expected(i))
    }
  }
}
