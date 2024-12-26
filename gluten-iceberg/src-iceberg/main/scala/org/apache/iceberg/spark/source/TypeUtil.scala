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
package org.apache.iceberg.spark.source

import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}

import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.TypeID

import java.lang.{Long => JLong}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset

object TypeUtil {
  def validatePartitionColumnType(typeID: TypeID): Unit = typeID match {
    case TypeID.BOOLEAN =>
    case TypeID.INTEGER =>
    case TypeID.LONG =>
    case TypeID.FLOAT =>
    case TypeID.DOUBLE =>
    case TypeID.DATE =>
    case TypeID.TIME | TypeID.TIMESTAMP =>
    case TypeID.STRING =>
    case TypeID.BINARY =>
    case TypeID.DECIMAL =>
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported partition column type $typeID")
  }

  def getPartitionValueString(partitionType: Type, partitionValue: Any): String = {
    partitionType.typeId() match {
      case TypeID.BINARY =>
        new String(partitionValue.asInstanceOf[ByteBuffer].array(), StandardCharsets.UTF_8)
      case TypeID.DATE =>
        DateFormatter.apply().format(partitionValue.asInstanceOf[Integer])
      case TypeID.TIMESTAMP | TypeID.TIME =>
        TimestampFormatter
          .getFractionFormatter(ZoneOffset.UTC)
          .format(partitionValue.asInstanceOf[JLong])
      case _ =>
        partitionValue.toString
    }
  }
}
