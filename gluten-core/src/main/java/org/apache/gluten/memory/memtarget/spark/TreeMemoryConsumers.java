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
package org.apache.gluten.memory.memtarget.spark;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import org.apache.commons.collections.map.ReferenceMap;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TreeMemoryConsumers {
  private static final ReferenceMap FACTORIES = new ReferenceMap();

  private TreeMemoryConsumers() {}

  @SuppressWarnings("unchecked")
  public static Factory factory(TaskMemoryManager tmm) {
    synchronized (FACTORIES) {
      return (Factory) FACTORIES.computeIfAbsent(tmm, m -> new Factory((TaskMemoryManager) m));
    }
  }

  public static class Factory {
    private final TreeMemoryConsumer sparkConsumer;
    private final Map<Long, TreeMemoryTarget> roots = new ConcurrentHashMap<>();

    private Factory(TaskMemoryManager tmm) {
      this.sparkConsumer = new TreeMemoryConsumer(tmm);
    }

    private TreeMemoryTarget ofCapacity(long capacity) {
      return roots.computeIfAbsent(
          capacity,
          cap ->
              sparkConsumer.newChild(
                  String.format("Capacity[%s]", Utils.bytesToString(cap)),
                  cap,
                  Spillers.NOOP,
                  Collections.emptyMap()));
    }

    /**
     * This works as a legacy Spark memory consumer which grants as much as possible of memory
     * capacity to each task.
     */
    public TreeMemoryTarget legacyRoot() {
      return ofCapacity(TreeMemoryTarget.CAPACITY_UNLIMITED);
    }

    /**
     * A hub to provide memory target instances whose shared size (in the same task) is limited to
     * X, X = executor memory / task slots.
     *
     * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
     * blocks that are not spill-able.
     *
     * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
     */
    public TreeMemoryTarget isolatedRoot() {
      return ofCapacity(GlutenConfig.getConf().conservativeTaskOffHeapMemorySize());
    }
  }
}
