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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{withDefaultTimeZone, ALL_TIMEZONES, UTC, UTC_OPT}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{fromJavaTimestamp, millisToMicros, TimeZoneUTC}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import java.util.Calendar

class GlutenCastSuite extends CastSuite with GlutenTestsTrait {
  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
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
      t =>
        t match {
          case BooleanType =>
            checkEvaluation(cast(cast(true, BooleanType), t), true)
            checkEvaluation(cast(cast(false, BooleanType), t), false)
          case _ =>
            checkEvaluation(cast(cast(true, BooleanType), t), 1)
            checkEvaluation(cast(cast(false, BooleanType), t), 0)
        }
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

  testGluten("data type casting") {
    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = withDefaultTimeZone(UTC)(Timestamp.valueOf(nts))

    for (tz <- ALL_TIMEZONES) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> tz.getId
      ) {
        val timeZoneId = Option(tz.getId)
        var c = Calendar.getInstance(TimeZoneUTC)
        c.set(2015, 2, 8, 2, 30, 0)
        checkEvaluation(
          cast(
            cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
            TimestampType,
            timeZoneId),
          millisToMicros(c.getTimeInMillis))
        c = Calendar.getInstance(TimeZoneUTC)
        c.set(2015, 10, 1, 2, 30, 0)
        checkEvaluation(
          cast(
            cast(new Timestamp(c.getTimeInMillis), StringType, timeZoneId),
            TimestampType,
            timeZoneId),
          millisToMicros(c.getTimeInMillis))
      }
    }

    checkEvaluation(cast("abdef", StringType), "abdef")
    checkEvaluation(cast("12.65", DecimalType.SYSTEM_DEFAULT), Decimal(12.65))

    checkEvaluation(cast(cast(sd, DateType), StringType), sd)
    checkEvaluation(cast(cast(d, StringType), DateType), 0)

    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC_OPT.get
    ) {
      checkEvaluation(cast(cast(nts, TimestampType, UTC_OPT), StringType, UTC_OPT), nts)
      checkEvaluation(
        cast(cast(ts, StringType, UTC_OPT), TimestampType, UTC_OPT),
        fromJavaTimestamp(ts))

      // all convert to string type to check
      checkEvaluation(
        cast(cast(cast(nts, TimestampType, UTC_OPT), DateType, UTC_OPT), StringType),
        sd)
      checkEvaluation(
        cast(cast(cast(ts, DateType, UTC_OPT), TimestampType, UTC_OPT), StringType, UTC_OPT),
        zts)
    }

    checkEvaluation(cast(cast("abdef", BinaryType), StringType), "abdef")

    checkEvaluation(
      cast(
        cast(cast(cast(cast(cast("5", ByteType), ShortType), IntegerType), FloatType), DoubleType),
        LongType),
      5.toLong)

    checkEvaluation(cast("23", DoubleType), 23d)
    checkEvaluation(cast("23", IntegerType), 23)
    checkEvaluation(cast("23", FloatType), 23f)
    checkEvaluation(cast("23", DecimalType.USER_DEFAULT), Decimal(23))
    checkEvaluation(cast("23", ByteType), 23.toByte)
    checkEvaluation(cast("23", ShortType), 23.toShort)
    checkEvaluation(cast(123, IntegerType), 123)

    checkEvaluation(cast(Literal.create(null, IntegerType), ShortType), null)
  }

  test("cast from double to timestamp format") {
    withSQLConf(
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> UTC_OPT.get
    ) {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

      checkEvaluation(
        cast(0.0, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 00:00:00")
      )

      checkEvaluation(
        cast(1.5, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 00:00:01.5")
      )

      checkEvaluation(
        cast(12345.6789, TimestampType, UTC_OPT),
        Timestamp.valueOf("1970-01-01 03:25:45.6789")
      )

      checkEvaluation(
        cast(-1.2, TimestampType, UTC_OPT),
        Timestamp.valueOf("1969-12-31 23:59:58.8")
      )
    }
  }
}
