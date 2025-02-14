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
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.{ALL_TIMEZONES, UTC}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.util.ThreadUtils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.{Calendar, TimeZone}

class GlutenCastSuiteWithAnsiModeOn extends AnsiCastSuiteBase with GlutenTestsTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => Cast(lit, targetType, timeZoneId)
      case _ => Cast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.ANSI_ENABLED.key} as false"
}

class GlutenAnsiCastSuiteWithAnsiModeOn extends AnsiCastSuiteBase with GlutenTestsTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, true)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => AnsiCast(lit, targetType, timeZoneId)
      case _ => AnsiCast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.STORE_ASSIGNMENT_POLICY.key} as" +
      s" ${SQLConf.StoreAssignmentPolicy.LEGACY.toString}"
}

class GlutenAnsiCastSuiteWithAnsiModeOff extends AnsiCastSuiteBase with GlutenTestsTrait {

  override def beforeAll(): Unit = {
    super.beforeAll()
    SQLConf.get.setConf(SQLConf.ANSI_ENABLED, false)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SQLConf.get.unsetConf(SQLConf.ANSI_ENABLED)
  }

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): CastBase = {
    v match {
      case lit: Expression => AnsiCast(lit, targetType, timeZoneId)
      case _ => AnsiCast(Literal(v), targetType, timeZoneId)
    }
  }

  override def setConfigurationHint: String =
    s"set ${SQLConf.STORE_ASSIGNMENT_POLICY.key} as" +
      s" ${SQLConf.StoreAssignmentPolicy.LEGACY.toString}"
}

class GlutenTryCastSuite extends TryCastSuite with GlutenTestsTrait {

  private val specialTs = Seq(
    "0001-01-01T00:00:00", // the fist timestamp of Common Era
    "1582-10-15T23:59:59", // the cutover date from Julian to Gregorian calendar
    "1970-01-01T00:00:00", // the epoch timestamp
    "9999-12-31T23:59:59" // the last supported timestamp according to SQL standard
  )

  testGluten("SPARK-35698: cast timestamp without time zone to string") {
    specialTs.foreach {
      s => checkEvaluation(cast(LocalDateTime.parse(s), StringType), s.replace("T", " "))
    }
  }

  testGluten("cast string to timestamp") {
    ThreadUtils.parmap(
      ALL_TIMEZONES,
      prefix = "CastSuiteBase-cast-string-to-timestamp",
      maxThreads = Runtime.getRuntime.availableProcessors
    ) {
      zid =>
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> zid.getId
        ) {
          def checkCastStringToTimestamp(str: String, expected: Timestamp): Unit = {
            checkEvaluation(cast(Literal(str), TimestampType, Option(zid.getId)), expected)
          }

          val tz = TimeZone.getTimeZone(zid)
          var c = Calendar.getInstance(tz)
          c.set(2015, 0, 1, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015", new Timestamp(c.getTimeInMillis))
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 1, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03", new Timestamp(c.getTimeInMillis))
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 0, 0, 0)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 ", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18 12:03:17", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17", new Timestamp(c.getTimeInMillis))

          // If the string value includes timezone string, it represents the timestamp string
          // in the timezone regardless of the timeZoneId parameter.
          c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17Z", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 12:03:17Z", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17-1:0", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17-01:00", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17+07:30", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 0)
          checkCastStringToTimestamp("2015-03-18T12:03:17+7:3", new Timestamp(c.getTimeInMillis))

          // tests for the string including milliseconds.
          c = Calendar.getInstance(tz)
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp("2015-03-18 12:03:17.123", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18T12:03:17.123", new Timestamp(c.getTimeInMillis))

          // If the string value includes timezone string, it represents the timestamp string
          // in the timezone regardless of the timeZoneId parameter.
          c = Calendar.getInstance(TimeZone.getTimeZone(UTC))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 456)
          checkCastStringToTimestamp("2015-03-18T12:03:17.456Z", new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp("2015-03-18 12:03:17.456Z", new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT-01:00"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123-1:0",
            new Timestamp(c.getTimeInMillis))
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123-01:00",
            new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:30"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123+07:30",
            new Timestamp(c.getTimeInMillis))

          c = Calendar.getInstance(TimeZone.getTimeZone("GMT+07:03"))
          c.set(2015, 2, 18, 12, 3, 17)
          c.set(Calendar.MILLISECOND, 123)
          checkCastStringToTimestamp(
            "2015-03-18T12:03:17.123+7:3",
            new Timestamp(c.getTimeInMillis))
        }
    }
  }
}
