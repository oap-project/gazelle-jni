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

import com.google.common.base.Preconditions;

public class OverAcquire implements MemoryTarget {

  // The underlying target.
  private final MemoryTarget target;

  // This consumer holds the over-acquired memory.
  private final MemoryTarget overTarget;

  // The ratio is normally 0.
  //
  // If set to some value other than 0, the consumer will try
  //   over-acquire this ratio of memory each time it acquires
  //   from Spark.
  //
  // Once OOM, the over-acquired memory will be used as backup.
  //
  // The over-acquire is a general workaround for underling reservation
  //   procedures that were not perfectly-designed for spilling. For example,
  //   reservation for a two-step procedure: step A is capable for
  //   spilling while step B is not. If not reserving enough memory
  //   for step B before it's started, it might raise OOM since step A
  //   is ended and no longer open for spilling. In this case the
  //   over-acquired memory will be used in step B.
  private final double ratio;

  OverAcquire(MemoryTarget target, MemoryTarget overTarget, double ratio) {
    Preconditions.checkArgument(ratio >= 0.0D);
    this.overTarget = overTarget;
    this.target = target;
    this.ratio = ratio;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }
    Preconditions.checkState(overTarget.usedBytes() == 0);
    long granted = target.borrow(size);
    if (granted >= size) {
      long majorSize = target.usedBytes();
      long overSize = (long) (ratio * majorSize);
      long overAcquired = overTarget.borrow(overSize);
      Preconditions.checkState(overAcquired == overTarget.usedBytes());
      long releasedOverSize = overTarget.repay(overAcquired);
      Preconditions.checkState(releasedOverSize == overAcquired);
      Preconditions.checkState(overTarget.usedBytes() == 0);
    }
    return granted;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes() + overTarget.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget getTarget() {
    return target;
  }
}
