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

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.Runtimes;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.SparkColumnarBatchUtil;

import java.util.Arrays;
import java.util.Objects;

public final class VeloxColumnarBatches {
  public static final String COMPREHENSIVE_TYPE_VELOX = "velox";

  private static boolean isVeloxBatch(ColumnarBatch batch) {
    final String comprehensiveType = ColumnarBatches.getComprehensiveLightBatchType(batch);
    return Objects.equals(comprehensiveType, COMPREHENSIVE_TYPE_VELOX);
  }

  public static void checkVeloxBatch(ColumnarBatch batch) {
    if (ColumnarBatches.isZeroColumnBatch(batch)) {
      return;
    }
    Preconditions.checkArgument(
        isVeloxBatch(batch),
        String.format(
            "Expected comprehensive batch type %s, but got %s",
            COMPREHENSIVE_TYPE_VELOX, ColumnarBatches.getComprehensiveLightBatchType(batch)));
  }

  public static void checkNonVeloxBatch(ColumnarBatch batch) {
    if (ColumnarBatches.isZeroColumnBatch(batch)) {
      return;
    }
    Preconditions.checkArgument(
        !isVeloxBatch(batch),
        String.format("Comprehensive batch type is already %s", COMPREHENSIVE_TYPE_VELOX));
  }

  public static ColumnarBatch toVeloxBatch(ColumnarBatch input) {
    if (ColumnarBatches.isZeroColumnBatch(input)) {
      return input;
    }
    Preconditions.checkArgument(!isVeloxBatch(input));
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "VeloxColumnarBatches#toVeloxBatch");
    final long handle = ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), input);
    final long outHandle = VeloxColumnarBatchJniWrapper.create(runtime).from(handle);
    final ColumnarBatch output = ColumnarBatches.create(outHandle);

    // Follow input's reference count. This might be optimized using
    // automatic clean-up or once the extensibility of ColumnarBatch is enriched
    final long refCnt = ColumnarBatches.getRefCntLight(input);
    final IndicatorVector giv = (IndicatorVector) output.column(0);
    for (long i = 0; i < (refCnt - 1); i++) {
      giv.retain();
    }

    // close the input one
    for (long i = 0; i < refCnt; i++) {
      input.close();
    }

    // Populate new vectors to input.
    SparkColumnarBatchUtil.transferVectors(output, input);

    return input;
  }

  /**
   * Combine multiple columnar batches horizontally, assuming each of them is already offloaded.
   * Otherwise {@link UnsupportedOperationException} will be thrown.
   */
  public static ColumnarBatch compose(ColumnarBatch... batches) {
    final Runtime runtime =
        Runtimes.contextInstance(
            BackendsApiManager.getBackendName(), "VeloxColumnarBatches#compose");
    final long[] handles =
        Arrays.stream(batches)
            .mapToLong(b -> ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName(), b))
            .toArray();
    final long handle = VeloxColumnarBatchJniWrapper.create(runtime).compose(handles);
    return ColumnarBatches.create(handle);
  }
}
