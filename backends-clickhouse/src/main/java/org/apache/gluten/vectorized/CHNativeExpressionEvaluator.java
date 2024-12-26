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
package org.apache.gluten.vectorized;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.execution.ColumnarNativeIterator;
import org.apache.gluten.memory.CHThreadGroup;
import org.apache.gluten.utils.ConfigUtil;

import org.apache.spark.sql.internal.SQLConf;

import java.util.List;
import java.util.Map;

public class CHNativeExpressionEvaluator extends ExpressionEvaluatorJniWrapper {

  private CHNativeExpressionEvaluator() {}

  // Used to initialize the native computing.
  public static void initNative(scala.collection.Map<String, String> conf) {
    Map<String, String> nativeConfMap =
        GlutenConfig.getNativeBackendConf(BackendsApiManager.getBackendName(), conf);

    // Get the customer config from SparkConf for each backend
    BackendsApiManager.getTransformerApiInstance()
        .postProcessNativeConfig(
            nativeConfMap, GlutenConfig.prefixOf(BackendsApiManager.getBackendName()));

    nativeInitNative(ConfigUtil.serialize(nativeConfMap));
  }

  public static void finalizeNative() {
    nativeFinalizeNative();
  }

  // Used to validate the Substrait plan in native compute engine.
  public static boolean doValidate(byte[] subPlan) {
    throw new UnsupportedOperationException("doValidate is not supported in Clickhouse Backend");
  }

  private static Map<String, String> getNativeBackendConf() {
    return GlutenConfig.getNativeBackendConf(
        BackendsApiManager.getBackendName(), SQLConf.get().getAllConfs());
  }

  // Used by WholeStageTransform to create the native computing pipeline and
  // return a columnar result iterator.
  public static BatchIterator createKernelWithBatchIterator(
      byte[] wsPlan,
      byte[][] splitInfo,
      List<ColumnarNativeIterator> iterList,
      boolean materializeInput) {
    CHThreadGroup.registerNewThreadGroup();
    long handle =
        nativeCreateKernelWithIterator(
            wsPlan,
            splitInfo,
            iterList.toArray(new ColumnarNativeIterator[0]),
            ConfigUtil.serialize(getNativeBackendConf()),
            materializeInput);
    return createBatchIterator(handle);
  }

  // Only for UT.
  public static BatchIterator createKernelWithBatchIterator(
      byte[] wsPlan, byte[][] splitInfo, List<ColumnarNativeIterator> iterList) {
    CHThreadGroup.registerNewThreadGroup();
    long handle =
        nativeCreateKernelWithIterator(
            wsPlan,
            splitInfo,
            iterList.toArray(new ColumnarNativeIterator[0]),
            ConfigUtil.serialize(getNativeBackendConf()),
            false);
    return createBatchIterator(handle);
  }

  private static BatchIterator createBatchIterator(long nativeHandle) {
    return new BatchIterator(nativeHandle);
  }
}
