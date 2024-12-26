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

import org.apache.gluten.memory.MemoryUsageRecorder;
import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.KnownNameAndStats;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.MemoryTargetUtil;
import org.apache.gluten.memory.memtarget.MemoryTargetVisitor;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.task.TaskResources;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * A trivial memory consumer implementation used by Gluten.
 *
 * @deprecated Use {@link TreeMemoryConsumers} instead.
 */
@Deprecated
public class RegularMemoryConsumer extends MemoryConsumer
    implements MemoryTarget, KnownNameAndStats {

  private final TaskMemoryManager taskMemoryManager;
  private final Spiller spiller;
  private final String name;
  private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
  private final MemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

  public RegularMemoryConsumer(
      TaskMemoryManager taskMemoryManager,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    this.taskMemoryManager = taskMemoryManager;
    this.spiller = spiller;
    this.name = MemoryTargetUtil.toUniqueName("Gluten.Regular." + name);
    this.virtualChildren = virtualChildren;
  }

  @Override
  public long spill(final long size, MemoryConsumer trigger) {
    long remainingBytes = size;
    for (Spiller.Phase phase : Spiller.Phase.values()) {
      // First shrink, then if no good, spill.
      if (remainingBytes <= 0) {
        break;
      }
      remainingBytes -= spiller.spill(this, phase, size);
    }
    long spilledOut = size - remainingBytes;
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMetrics().incMemoryBytesSpilled(spilledOut);
    }
    return spilledOut;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public MemoryUsageStats stats() {
    MemoryUsageStats stats = this.selfRecorder.toStats();
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    // In the case of Velox backend, stats returned from C++ (as the singleton virtual children,
    // currently) may not include all allocations according to the way collectMemoryUsage()
    // is implemented. So we add them as children of this consumer's stats
    return MemoryUsageStats.newBuilder()
        .setCurrent(stats.getCurrent())
        .setPeak(stats.getPeak())
        .putAllChildren(
            virtualChildren.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toStats())))
        .build();
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    selfRecorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    long toFree = Math.min(getUsed(), size);
    freeMemory(toFree);
    Preconditions.checkArgument(getUsed() >= 0);
    selfRecorder.inc(-toFree);
    return toFree;
  }

  @Override
  public String toString() {
    return name();
  }

  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }
}
