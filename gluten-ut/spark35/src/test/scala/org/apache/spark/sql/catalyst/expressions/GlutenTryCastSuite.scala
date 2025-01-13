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
import org.apache.spark.sql.types.{BinaryType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

import java.sql.{Date, Timestamp}
import java.util.Calendar

class GlutenTryCastSuite extends TryCastSuite with GlutenTestsTrait {

  testGluten("data type casting") {
    val sd = "1970-01-01"
    val d = Date.valueOf(sd)
    val zts = sd + " 00:00:00"
    val sts = sd + " 00:00:02"
    val nts = sts + ".1"
    val ts = withDefaultTimeZone(UTC)(Timestamp.valueOf(nts))

    // SystemV timezones are a legacy way of specifying timezones in Unix-like OS.
    // It is not supported by Velox.
    for (tz <- ALL_TIMEZONES.filterNot(_.getId.contains("SystemV"))) {
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

}
