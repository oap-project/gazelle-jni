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
package org.apache.gluten.softaffinity.strategy

import org.apache.gluten.hash.ConsistentHash

import java.util.Objects

import scala.collection.JavaConverters._

class ConsistentHashSoftAffinityStrategy(candidates: ConsistentHash[ExecutorNode])
  extends SoftAffinityAllocationTrait {

  /** Allocate the executors of count number from the candidates. */
  override def allocateExecs(file: String, count: Int): Array[(String, String)] = {
    candidates.allocateNodes(file, count).asScala.map(node => (node.exeId, node.host)).toArray
  }
}

case class ExecutorNode(exeId: String, host: String) extends ConsistentHash.Node {
  override def key(): String = s"$exeId-$host"

  override def toString: String = s"$exeId-$host"

  override def equals(o: Any): Boolean = {
    o match {
      case ExecutorNode(exeId, host) => this.exeId == exeId && this.host == host
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(exeId, host)
}
