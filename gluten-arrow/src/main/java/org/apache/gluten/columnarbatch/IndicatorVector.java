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
package org.apache.gluten.columnarbatch;

import org.apache.spark.task.TaskResources;

import java.util.concurrent.atomic.AtomicLong;

public class IndicatorVector extends IndicatorVectorBase {
  private final IndicatorVectorPool pool;
  private final AtomicLong refCnt = new AtomicLong(1L);

  protected IndicatorVector(IndicatorVectorPool pool, long handle) {
    super(handle);
    this.pool = pool;
  }

  static IndicatorVector obtain(long handle) {
    final IndicatorVectorPool pool =
        TaskResources.addResourceIfNotRegistered(
            IndicatorVectorPool.class.getName(), IndicatorVectorPool::new);
    return pool.obtain(handle);
  }

  @Override
  long refCnt() {
    return refCnt.get();
  }

  @Override
  void retain() {
    refCnt.getAndIncrement();
  }

  @Override
  void release() {
    if (refCnt.get() == 0) {
      // TODO use stronger restriction (IllegalStateException probably)
      return;
    }
    if (refCnt.decrementAndGet() == 0) {
      pool.remove(handle);
      ColumnarBatchJniWrapper.close(handle);
    }
  }
}
