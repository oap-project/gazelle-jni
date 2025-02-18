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

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.sql.vectorized.ColumnarRow;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

// Copy from Spark MutableColumnarRow mostly but class member columns`type is
// ArrowWritableColumnVector. And support string and binary type to write, Arrow writer does not
// need to setNotNull before write a value.
public final class ArrowColumnarRow extends InternalRow {
  public int rowId;
  private final ArrowWritableColumnVector[] columns;

  public ArrowColumnarRow(ArrowWritableColumnVector[] writableColumns) {
    this.columns = writableColumns;
  }

  @Override
  public int numFields() {
    return columns.length;
  }

  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(columns.length);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = columns[i].dataType();
        if (dt instanceof BooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (dt instanceof ByteType) {
          row.setByte(i, getByte(i));
        } else if (dt instanceof ShortType) {
          row.setShort(i, getShort(i));
        } else if (dt instanceof IntegerType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof LongType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof FloatType) {
          row.setFloat(i, getFloat(i));
        } else if (dt instanceof DoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (dt instanceof StringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (dt instanceof BinaryType) {
          row.update(i, getBinary(i));
        } else if (dt instanceof DecimalType) {
          DecimalType t = (DecimalType) dt;
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (dt instanceof DateType) {
          row.setInt(i, getInt(i));
        } else if (dt instanceof TimestampType) {
          row.setLong(i, getLong(i));
        } else if (dt instanceof StructType) {
          row.update(i, getStruct(i, ((StructType) dt).fields().length).copy());
        } else if (dt instanceof ArrayType) {
          row.update(i, getArray(i).copy());
        } else if (dt instanceof MapType) {
          row.update(i, getMap(i).copy());
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return columns[ordinal].isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return columns[ordinal].getBoolean(rowId);
  }

  @Override
  public byte getByte(int ordinal) {
    return columns[ordinal].getByte(rowId);
  }

  @Override
  public short getShort(int ordinal) {
    return columns[ordinal].getShort(rowId);
  }

  @Override
  public int getInt(int ordinal) {
    return columns[ordinal].getInt(rowId);
  }

  @Override
  public long getLong(int ordinal) {
    return columns[ordinal].getLong(rowId);
  }

  @Override
  public float getFloat(int ordinal) {
    return columns[ordinal].getFloat(rowId);
  }

  @Override
  public double getDouble(int ordinal) {
    return columns[ordinal].getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[ordinal].getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[ordinal].getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return columns[ordinal].getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return columns[ordinal].getInterval(rowId);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return columns[ordinal].getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return columns[ordinal].getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return columns[ordinal].getMap(rowId);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType t = (DecimalType) dataType;
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType) dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setNullAt(int ordinal) {
    columns[ordinal].putNull(rowId);
  }

  @Override
  public void setBoolean(int ordinal, boolean value) {
    columns[ordinal].putBoolean(rowId, value);
  }

  @Override
  public void setByte(int ordinal, byte value) {
    columns[ordinal].putByte(rowId, value);
  }

  @Override
  public void setShort(int ordinal, short value) {
    columns[ordinal].putShort(rowId, value);
  }

  @Override
  public void setInt(int ordinal, int value) {
    columns[ordinal].putInt(rowId, value);
  }

  @Override
  public void setLong(int ordinal, long value) {
    columns[ordinal].putLong(rowId, value);
  }

  @Override
  public void setFloat(int ordinal, float value) {
    columns[ordinal].putFloat(rowId, value);
  }

  @Override
  public void setDouble(int ordinal, double value) {
    columns[ordinal].putDouble(rowId, value);
  }

  @Override
  public void setDecimal(int ordinal, Decimal value, int precision) {
    columns[ordinal].putDecimal(rowId, value, precision);
  }

  @Override
  public void setInterval(int ordinal, CalendarInterval value) {
    columns[ordinal].putInterval(rowId, value);
  }

  public void setUTF8String(int ordinal, UTF8String value) {
    columns[ordinal].putBytes(rowId, value.numBytes(), value.getBytes(), 0);
  }

  public void setBinary(int ordinal, byte[] value) {
    columns[ordinal].putBytes(rowId, value.length, value, 0);
  }

  public void writeRow(GenericInternalRow input) {
    if (input.numFields() != columns.length) {
      throw new GlutenException(
          "The numFields of input row should be equal to the number of column vector!");
    }
    for (int i = 0; i < input.numFields(); ++i) {
      columns[i].write(input, i);
    }
  }

  public void finishWriteRow() {
    for (int i = 0; i < columns.length; ++i) {
      columns[i].finishWrite();
    }
  }
}
