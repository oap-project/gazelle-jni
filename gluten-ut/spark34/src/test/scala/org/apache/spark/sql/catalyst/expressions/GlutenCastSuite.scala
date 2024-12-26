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
import org.apache.spark.sql.types._

import java.sql.Date

class GlutenCastSuite extends CastSuiteBase with GlutenTestsTrait {
  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression =>
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
      case _ =>
        val lit = Literal(v)
        logDebug(s"Cast from: ${lit.dataType.typeName}, to: ${targetType.typeName}")
        Cast(lit, targetType, timeZoneId)
    }
  }

  // Register UDT For test("SPARK-32828")
  UDTRegistration.register(classOf[IExampleBaseType].getName, classOf[ExampleBaseTypeUDT].getName)
  UDTRegistration.register(classOf[IExampleSubType].getName, classOf[ExampleSubTypeUDT].getName)

  testGluten("missing cases - from boolean") {
    (DataTypeTestUtils.numericTypeWithoutDecimal + BooleanType).foreach {
      case t @ BooleanType =>
        checkEvaluation(cast(cast(true, BooleanType), t), true)
        checkEvaluation(cast(cast(false, BooleanType), t), false)
      case t =>
        checkEvaluation(cast(cast(true, BooleanType), t), 1)
        checkEvaluation(cast(cast(false, BooleanType), t), 0)
    }
  }

  testGluten("missing cases - from byte") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ByteType), t), 0)
        checkEvaluation(cast(cast(-1, ByteType), t), -1)
        checkEvaluation(cast(cast(1, ByteType), t), 1)
    }
  }

  testGluten("missing cases - from short") {
    DataTypeTestUtils.numericTypeWithoutDecimal.foreach {
      t =>
        checkEvaluation(cast(cast(0, ShortType), t), 0)
        checkEvaluation(cast(cast(-1, ShortType), t), -1)
        checkEvaluation(cast(cast(1, ShortType), t), 1)
    }
  }

  testGluten("missing cases - date self check") {
    val d = Date.valueOf("1970-01-01")
    checkEvaluation(cast(d, DateType), d)
  }

  override protected def evalMode: EvalMode.Value = EvalMode.LEGACY
}
