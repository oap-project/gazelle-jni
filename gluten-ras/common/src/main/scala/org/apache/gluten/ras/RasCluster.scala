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
package org.apache.gluten.ras

import org.apache.gluten.ras.Ras.UnsafeHashKey
import org.apache.gluten.ras.memo.MemoTable
import org.apache.gluten.ras.property.PropertySet

import scala.collection.mutable

trait RasClusterKey {
  def metadata: Metadata
}

object RasClusterKey {
  implicit class RasClusterKeyImplicits(key: RasClusterKey) {
    def propSets[T <: AnyRef](memoTable: MemoTable[T]): Set[PropertySet[T]] = {
      memoTable.getClusterPropSets(key)
    }
  }
}

trait RasCluster[T <: AnyRef] {
  def nodes(): Iterable[CanonicalNode[T]]
}

object RasCluster {
  // Node cluster.
  trait MutableRasCluster[T <: AnyRef] extends RasCluster[T] {
    def ras(): Ras[T]
    def contains(t: CanonicalNode[T]): Boolean
    def add(t: CanonicalNode[T]): Unit
  }

  object MutableRasCluster {
    def apply[T <: AnyRef](ras: Ras[T], metadata: Metadata): MutableRasCluster[T] = {
      new RegularMutableRasCluster(ras, metadata)
    }

    private class RegularMutableRasCluster[T <: AnyRef](
        override val ras: Ras[T],
        metadata: Metadata)
      extends MutableRasCluster[T] {
      private val deDup: mutable.Set[UnsafeHashKey[T]] = mutable.Set()
      private val buffer: mutable.ListBuffer[CanonicalNode[T]] =
        mutable.ListBuffer()

      override def contains(t: CanonicalNode[T]): Boolean = {
        deDup.contains(t.toHashKey())
      }

      override def add(t: CanonicalNode[T]): Unit = {
        val key = t.toHashKey()
        assert(!deDup.contains(key))
        ras.metadataModel.verify(metadata, ras.metadataModel.metadataOf(t.self()))
        deDup += key
        buffer += t
      }

      override def nodes(): Iterable[CanonicalNode[T]] = {
        buffer
      }
    }
  }

  case class ImmutableRasCluster[T <: AnyRef] private (
      ras: Ras[T],
      override val nodes: Seq[CanonicalNode[T]])
    extends RasCluster[T]

  object ImmutableRasCluster {
    def apply[T <: AnyRef](ras: Ras[T], cluster: RasCluster[T]): ImmutableRasCluster[T] = {
      ImmutableRasCluster(ras, cluster.nodes().toVector)
    }
  }
}
