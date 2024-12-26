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
package org.apache.spark.shuffle

import org.apache.celeborn.client.ShuffleClient
import org.apache.spark._
import org.apache.spark.internal.Logging

import java.io.IOException

class CelebornPartitionPusher(
    val shuffleId: Int,
    val numMappers: Int,
    val numPartitions: Int,
    val context: TaskContext,
    val mapId: Int,
    val client: ShuffleClient,
    val clientPushBufferMaxSize: Int)
  extends Logging {

  @throws[IOException]
  def pushPartitionData(partitionId: Int, buffer: Array[Byte], length: Int): Int = {
    if (length > clientPushBufferMaxSize) {
      logDebug(s"Push Data, size $length.")
      client.pushData(
        shuffleId,
        mapId,
        context.attemptNumber,
        partitionId,
        buffer,
        0,
        length,
        numMappers,
        numPartitions)
    } else {
      logDebug(s"Merge Data, size $length.")
      client.mergeData(
        shuffleId,
        mapId,
        context.attemptNumber,
        partitionId,
        buffer,
        0,
        length,
        numMappers,
        numPartitions)
    }
  }
}
