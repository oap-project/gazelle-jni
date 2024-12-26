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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.memory.MemoryUsageStatsBuilder;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

public class TreeMemoryTargets {

  private TreeMemoryTargets() {
    // enclose factory ctor
  }

  /**
   * A short-cut method to create a child target of `parent`. The child will follow the parent's
   * maximum capacity.
   */
  static TreeMemoryTarget newChild(
      TreeMemoryTarget parent,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return parent.newChild(name, TreeMemoryTarget.CAPACITY_UNLIMITED, spiller, virtualChildren);
  }

  public static long spillTree(TreeMemoryTarget node, final long bytes) {
    long remainingBytes = bytes;
    for (Spiller.Phase phase : Spiller.Phase.values()) {
      // First shrink, then if no good, spill.
      if (remainingBytes <= 0) {
        break;
      }
      remainingBytes -= spillTree(node, phase, remainingBytes);
    }
    return bytes - remainingBytes;
  }

  private static long spillTree(TreeMemoryTarget node, Spiller.Phase phase, final long bytes) {
    // sort children by used bytes, descending
    Queue<TreeMemoryTarget> q =
        new PriorityQueue<>(
            (o1, o2) -> {
              long diff = o1.usedBytes() - o2.usedBytes();
              return -(diff > 0 ? 1 : diff < 0 ? -1 : 0); // descending
            });
    q.addAll(node.children().values());

    long remainingBytes = bytes;
    while (q.peek() != null && remainingBytes > 0) {
      TreeMemoryTarget head = q.remove();
      long spilled = spillTree(head, phase, remainingBytes);
      remainingBytes -= spilled;
    }

    if (remainingBytes > 0) {
      // if still doesn't fit, spill self
      final Spiller spiller = node.getNodeSpiller();
      long spilled = spiller.spill(node, phase, remainingBytes);
      remainingBytes -= spilled;
    }

    return bytes - remainingBytes;
  }
}
