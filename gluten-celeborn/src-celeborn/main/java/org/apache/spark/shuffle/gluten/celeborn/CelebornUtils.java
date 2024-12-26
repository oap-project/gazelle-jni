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
package org.apache.spark.shuffle.gluten.celeborn;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.spark.MapOutputTrackerMaster;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.DeterministicLevel;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle;
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.function.Consumer;

public class CelebornUtils {

  private static final Logger logger = LoggerFactory.getLogger(CelebornUtils.class);

  public static final String EXECUTOR_SHUFFLE_ID_TRACKER_NAME =
      "org.apache.spark.shuffle.celeborn.ExecutorShuffleIdTracker";

  public static boolean unregisterShuffle(
      LifecycleManager lifecycleManager,
      ShuffleClient shuffleClient,
      Object shuffleIdTracker,
      int appShuffleId,
      String appUniqueId,
      boolean throwsFetchFailure,
      boolean isDriver) {
    try {
      try {
        try {
          // for Celeborn 0.4.1
          if (lifecycleManager != null) {
            Method unregisterAppShuffle =
                lifecycleManager
                    .getClass()
                    .getMethod("unregisterAppShuffle", int.class, boolean.class);
            unregisterAppShuffle.invoke(lifecycleManager, appShuffleId, throwsFetchFailure);
          }
        } catch (NoSuchMethodException ex) {
          // for Celeborn 0.4.0
          Method unregisterAppShuffle =
              lifecycleManager.getClass().getMethod("unregisterAppShuffle", int.class);
          unregisterAppShuffle.invoke(lifecycleManager, appShuffleId);
        }
        if (shuffleClient != null) {
          Method unregisterAppShuffleId =
              Class.forName(EXECUTOR_SHUFFLE_ID_TRACKER_NAME)
                  .getMethod("unregisterAppShuffleId", ShuffleClient.class, int.class);
          unregisterAppShuffleId.invoke(shuffleIdTracker, shuffleClient, appShuffleId);
        }
        return true;
      } catch (NoSuchMethodException | ClassNotFoundException ex) {
        try {
          if (lifecycleManager != null) {
            Method unregisterShuffleMethod =
                lifecycleManager.getClass().getMethod("unregisterShuffle", int.class);
            unregisterShuffleMethod.invoke(lifecycleManager, appShuffleId);
          }
          if (shuffleClient != null) {
            Method cleanupShuffleMethod =
                shuffleClient.getClass().getMethod("cleanupShuffle", int.class);
            cleanupShuffleMethod.invoke(shuffleClient, appShuffleId);
          }
          return true;
        } catch (NoSuchMethodException ex1) {
          // for Celeborn 0.3.0 and 0.3.1
          if (appUniqueId == null) {
            return true;
          }

          if (shuffleClient == null) {
            return false;
          }
          Method unregisterShuffleMethod =
              shuffleClient.getClass().getMethod("unregisterShuffle", int.class, boolean.class);
          Object result = unregisterShuffleMethod.invoke(shuffleClient, appShuffleId, isDriver);
          return (Boolean) result;
        }
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static ShuffleClient getShuffleClient(
      String appUniqueId,
      String lifecycleManagerHost,
      Integer lifecycleManagerPort,
      CelebornConf conf,
      UserIdentifier userIdentifier,
      Boolean isDriver,
      byte[] extension) {
    try {
      try {
        try {
          Method method =
              // for Celeborn 0.4.0
              ShuffleClient.class.getDeclaredMethod(
                  "get",
                  String.class,
                  String.class,
                  int.class,
                  CelebornConf.class,
                  UserIdentifier.class,
                  byte[].class);
          return (ShuffleClient)
              method.invoke(
                  null,
                  appUniqueId,
                  lifecycleManagerHost,
                  lifecycleManagerPort,
                  conf,
                  userIdentifier,
                  extension);
        } catch (NoSuchMethodException noMethod) {
          Method method =
              // for Celeborn 0.3.1 and above, see CELEBORN-804
              ShuffleClient.class.getDeclaredMethod(
                  "get",
                  String.class,
                  String.class,
                  int.class,
                  CelebornConf.class,
                  UserIdentifier.class);
          return (ShuffleClient)
              method.invoke(
                  null,
                  appUniqueId,
                  lifecycleManagerHost,
                  lifecycleManagerPort,
                  conf,
                  userIdentifier);
        }
      } catch (NoSuchMethodException noMethod) {
        Method method =
            // for Celeborn 0.3.0, see CELEBORN-798
            ShuffleClient.class.getDeclaredMethod(
                "get",
                String.class,
                String.class,
                int.class,
                CelebornConf.class,
                UserIdentifier.class,
                boolean.class);
        return (ShuffleClient)
            method.invoke(
                null,
                appUniqueId,
                lifecycleManagerHost,
                lifecycleManagerPort,
                conf,
                userIdentifier,
                isDriver);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static Object createInstance(String className) {
    try {
      try {
        Class<?> clazz = Class.forName(className);

        Constructor<?> constructor = clazz.getConstructor();

        return constructor.newInstance();

      } catch (ClassNotFoundException e) {
        return null;
      }
    } catch (Exception rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static CelebornShuffleHandle getCelebornShuffleHandle(
      String appUniqueId,
      String lifecycleManagerHost,
      int lifecycleManagerPort,
      UserIdentifier userIdentifier,
      int shuffleId,
      boolean throwsFetchFailure,
      int numMappers,
      ShuffleDependency dependency) {
    try {
      try {
        Constructor<CelebornShuffleHandle> constructor =
            CelebornShuffleHandle.class.getConstructor(
                String.class,
                String.class,
                int.class,
                UserIdentifier.class,
                int.class,
                boolean.class,
                int.class,
                ShuffleDependency.class);

        return constructor.newInstance(
            appUniqueId,
            lifecycleManagerHost,
            lifecycleManagerPort,
            userIdentifier,
            shuffleId,
            throwsFetchFailure,
            numMappers,
            dependency);
      } catch (NoSuchMethodException noMethod) {
        Constructor<CelebornShuffleHandle> constructor =
            CelebornShuffleHandle.class.getConstructor(
                String.class,
                String.class,
                int.class,
                UserIdentifier.class,
                int.class,
                int.class,
                ShuffleDependency.class);

        return constructor.newInstance(
            appUniqueId,
            lifecycleManagerHost,
            lifecycleManagerPort,
            userIdentifier,
            shuffleId,
            numMappers,
            dependency);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static CelebornShuffleReader getCelebornShuffleReader(
      CelebornShuffleHandle handle,
      int startPartition,
      int endPartition,
      int startMapIndex,
      int endMapIndex,
      TaskContext context,
      CelebornConf conf,
      ShuffleReadMetricsReporter metrics,
      Object shuffleIdTracker) {
    try {
      try {
        // for Celeborn 0.4.0
        Constructor<CelebornShuffleReader> constructor =
            CelebornShuffleReader.class.getConstructor(
                CelebornShuffleHandle.class,
                int.class,
                int.class,
                int.class,
                int.class,
                TaskContext.class,
                CelebornConf.class,
                ShuffleReadMetricsReporter.class,
                getClassOrDefault(EXECUTOR_SHUFFLE_ID_TRACKER_NAME));

        return constructor.newInstance(
            handle,
            startPartition,
            endPartition,
            startMapIndex,
            endMapIndex,
            context,
            conf,
            metrics,
            shuffleIdTracker);
      } catch (NoSuchMethodException noMethod) {
        // for celeborn 0.3.x
        Constructor<CelebornShuffleReader> constructor =
            CelebornShuffleReader.class.getConstructor(
                CelebornShuffleHandle.class,
                int.class,
                int.class,
                int.class,
                int.class,
                TaskContext.class,
                CelebornConf.class,
                ShuffleReadMetricsReporter.class);

        return constructor.newInstance(
            handle,
            startPartition,
            endPartition,
            startMapIndex,
            endMapIndex,
            context,
            conf,
            metrics);
      }
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  public static Class<?> getClassOrDefault(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      return Object.class;
    }
  }

  public static boolean getThrowsFetchFailure(CelebornConf celebornConf) {
    try {
      Method clientFetchThrowsFetchFailureMethod =
          celebornConf.getClass().getDeclaredMethod("clientFetchThrowsFetchFailure");
      return (Boolean) clientFetchThrowsFetchFailureMethod.invoke(celebornConf);
    } catch (NoSuchMethodException e) {
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerShuffleTrackerCallback(
      boolean throwsFetchFailure, LifecycleManager lifecycleManager) {
    try {
      if (throwsFetchFailure) {
        MapOutputTrackerMaster mapOutputTracker =
            (MapOutputTrackerMaster) SparkEnv.get().mapOutputTracker();

        Method registerShuffleTrackerCallbackMethod =
            lifecycleManager
                .getClass()
                .getDeclaredMethod("registerShuffleTrackerCallback", Consumer.class);

        Consumer<Integer> consumer =
            shuffleId -> {
              try {
                Method unregisterAllMapOutputMethod =
                    SparkUtils.class.getMethod(
                        "unregisterAllMapOutput", MapOutputTrackerMaster.class, int.class);
                unregisterAllMapOutputMethod.invoke(null, mapOutputTracker, shuffleId);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            };

        registerShuffleTrackerCallbackMethod.invoke(lifecycleManager, consumer);
      }
    } catch (NoSuchMethodException e) {
      logger.debug("Executing the initializeLifecycleManager of celeborn-0.3.x");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void registerAppShuffleDeterminate(
      LifecycleManager lifecycleManager, int shuffleId, ShuffleDependency dependency) {
    try {
      Method registerAppShuffleDeterminateMethod =
          lifecycleManager
              .getClass()
              .getDeclaredMethod("registerAppShuffleDeterminate", Integer.TYPE, Boolean.TYPE);

      registerAppShuffleDeterminateMethod.invoke(
          lifecycleManager,
          shuffleId,
          dependency.rdd().getOutputDeterministicLevel() != DeterministicLevel.INDETERMINATE());

    } catch (NoSuchMethodException e) {
      logger.debug("Executing the registerShuffle of celeborn-0.3.x");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
