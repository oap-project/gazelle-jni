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

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public abstract class IndicatorVectorBase extends ColumnVector {
  protected final long handle;

  protected IndicatorVectorBase(long handle) {
    super(DataTypes.NullType);
    this.handle = handle;
  }

  public String getType() {
    return ColumnarBatchJniWrapper.getType(handle);
  }

  public long getNumColumns() {
    return ColumnarBatchJniWrapper.numColumns(handle);
  }

  public long getNumRows() {
    return ColumnarBatchJniWrapper.numRows(handle);
  }

  abstract long refCnt();

  abstract void retain();

  abstract void release();

  @Override
  public void close() {
    release();
  }

  @Override
  public boolean hasNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int numNulls() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException();
  }

  public long handle() {
    return handle;
  }
}
