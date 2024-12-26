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

import org.apache.spark.ShuffleDependency

private class ShuffleManagerLookup(all: Seq[(LookupKey, ShuffleManager)]) {
  private val allReversed = all.reverse

  def findShuffleManager[K, V, C](dependency: ShuffleDependency[K, V, C]): ShuffleManager = {
    this.synchronized {
      // The latest shuffle manager registered will be looked up earlier.
      allReversed.find(_._1.accepts(dependency)).map(_._2).getOrElse {
        throw new IllegalStateException(s"No ShuffleManager found for $dependency")
      }
    }
  }

  def all(): Seq[ShuffleManager] = {
    this.synchronized {
      all.map(_._2)
    }
  }
}
