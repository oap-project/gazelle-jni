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
package org.apache.gluten.utils;

import org.apache.gluten.backendsapi.BackendsApiManager;
import org.apache.gluten.runtime.Runtimes;

import org.apache.commons.io.IOUtils;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class VeloxBloomFilter extends BloomFilter {
  private final VeloxBloomFilterJniWrapper jni =
      VeloxBloomFilterJniWrapper.create(
          Runtimes.contextInstance(BackendsApiManager.getBackendName(), "VeloxBloomFilter"));
  private final long handle;

  private VeloxBloomFilter(byte[] data) {
    handle = jni.init(data);
  }

  private VeloxBloomFilter(int capacity) {
    handle = jni.empty(capacity);
  }

  public static VeloxBloomFilter empty(int capacity) {
    return new VeloxBloomFilter(capacity);
  }

  public static VeloxBloomFilter readFrom(InputStream in) {
    try {
      byte[] all = IOUtils.toByteArray(in);
      return new VeloxBloomFilter(all);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static VeloxBloomFilter readFrom(byte[] data) {
    try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
      return readFrom(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] serialize() {
    try (ByteArrayOutputStream o = new ByteArrayOutputStream()) {
      writeTo(o);
      return o.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double expectedFpp() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public long bitSize() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean put(Object item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean putString(String item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean putLong(long item) {
    jni.insertLong(handle, item);
    return true;
  }

  @Override
  public boolean putBinary(byte[] item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean isCompatible(BloomFilter other) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException {
    if (!(other instanceof VeloxBloomFilter)) {
      throw new IncompatibleMergeException(
          "Cannot merge Velox bloom-filter with non-Velox bloom-filter");
    }
    final VeloxBloomFilter from = (VeloxBloomFilter) other;

    if (!jni.isCompatibleWith(from.jni)) {
      throw new IncompatibleMergeException(
          "Cannot merge Velox bloom-filters with different Velox runtimes");
    }
    jni.mergeFrom(handle, from.handle);
    return this;
  }

  @Override
  public BloomFilter intersectInPlace(BloomFilter other) throws IncompatibleMergeException {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContain(Object item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContainString(String item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public boolean mightContainLong(long item) {
    return jni.mightContainLong(handle, item);
  }

  @Override
  public boolean mightContainBinary(byte[] item) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void writeTo(OutputStream out) throws IOException {
    byte[] data = jni.serialize(handle);
    out.write(data);
  }
}
