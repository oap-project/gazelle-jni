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
package org.apache.spark.shuffle.gluten.uniffle;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.ColumnarShuffleDependency;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.writer.VeloxUniffleColumnarShuffleWriter;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniffleShuffleManager extends RssShuffleManager {
  private static final Logger LOG = LoggerFactory.getLogger(UniffleShuffleManager.class);

  public UniffleShuffleManager(SparkConf conf, boolean isDriver) {
    super(conf, isDriver);
    conf.set(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_ROW_BASED.key(), "false");
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    if (!(handle instanceof RssShuffleHandle)) {
      throw new RssException("Unexpected ShuffleHandle:" + handle.getClass().getName());
    }
    RssShuffleHandle<K, V, V> rssHandle = (RssShuffleHandle<K, V, V>) handle;
    if (rssHandle.getDependency() instanceof ColumnarShuffleDependency) {
      ColumnarShuffleDependency<K, V, V> dependency =
          (ColumnarShuffleDependency<K, V, V>) rssHandle.getDependency();
      setPusherAppId(rssHandle);
      String taskId = context.taskAttemptId() + "_" + context.attemptNumber();
      ShuffleWriteMetrics writeMetrics;
      if (metrics != null) {
        writeMetrics = new WriteMetrics(metrics);
      } else {
        writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      }
      sparkConf.set(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_ROW_BASED.key(), "false");
      return new VeloxUniffleColumnarShuffleWriter<>(
          context.partitionId(),
          rssHandle.getAppId(),
          rssHandle.getShuffleId(),
          taskId,
          context.taskAttemptId(),
          writeMetrics,
          this,
          sparkConf,
          shuffleWriteClient,
          rssHandle,
          this::markFailedTask,
          context,
          dependency.isSort());
    } else {
      sparkConf.set(RssSparkConfig.SPARK_RSS_CONFIG_PREFIX + RssSparkConfig.RSS_ROW_BASED.key(), "true");
      return super.getWriter(handle, mapId, context, metrics);
    }
  }
}
