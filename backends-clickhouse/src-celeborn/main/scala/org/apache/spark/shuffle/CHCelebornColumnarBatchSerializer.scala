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
package org.apache.spark.shuffle

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.vectorized.BlockOutputStream
import org.apache.gluten.vectorized.CHStreamReader

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.celeborn.client.read.CelebornInputStream

import java.io._
import java.nio.ByteBuffer
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

class CHCelebornColumnarBatchSerializer(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new CHCelebornColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows, dataSize)
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class CHCelebornColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric)
  extends SerializerInstance
  with Logging {

  private lazy val conf = SparkEnv.get.conf
  private lazy val gluten_conf = GlutenConfig.getConf
  private lazy val compressionCodec = GlutenShuffleUtils.getCompressionCodec(conf)
  private lazy val capitalizedCompressionCodec = compressionCodec.toUpperCase(Locale.ROOT)
  private lazy val compressionLevel =
    GlutenShuffleUtils.getCompressionLevel(
      conf,
      compressionCodec,
      GlutenConfig.getConf.columnarShuffleCodecBackend.orNull)

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {
      private var reader: CHStreamReader = null
      private val original_in: InputStream = if (in.equals(CelebornInputStream.empty())) {
        null
      } else {
        in
      }
      private var cb: ColumnarBatch = _
      private val isEmptyStream: Boolean = in.equals(CelebornInputStream.empty())
      private val forceCompress: Boolean =
        gluten_conf.isUseColumnarShuffleManager ||
          gluten_conf.isUseCelebornShuffleManager

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      // Otherwise calling close() twice would cause replication of metrics.
      private val closeCalled: AtomicBoolean = new AtomicBoolean(false)

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def asKeyValueIterator: Iterator[(Any, Any)] = new Iterator[(Any, Any)] {
        private var gotNext = false
        private var nextValue: (Any, Any) = _
        private var finished = false

        def getNext: (Any, Any) = {
          try {
            (readKey[Any](), readValue[Any]())
          } catch {
            case eof: EOFException =>
              finished = true
              // try to release memory immediately
              closeReader()
              null
          }
        }

        override def hasNext: Boolean = {
          if (!isEmptyStream && !finished) {
            if (!gotNext) {
              nextValue = getNext
              gotNext = true
            }
          }
          !isEmptyStream && !finished
        }

        override def next(): (Any, Any) = {
          if (!hasNext) {
            throw new NoSuchElementException("End of stream")
          }
          gotNext = false
          nextValue
        }
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        if (cb != null) {
          cb.close()
          cb = null
        }

        var nativeBlock = getReader.next()
        while (nativeBlock.numRows() == 0) {
          if (nativeBlock.numColumns() == 0) {
            nativeBlock.close()
            this.close()
            throw new EOFException
          }
          nativeBlock = getReader.next()
        }
        val numRows = nativeBlock.numRows()

        numBatchesTotal += 1
        numRowsTotal += numRows
        cb = nativeBlock.toColumnarBatch
        cb.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (!closeCalled.compareAndSet(false, true)) {
          return
        }
        if (numBatchesTotal > 0) {
          readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
        }
        numOutputRows += numRowsTotal
        if (cb != null) {
          cb.close()
          cb = null
        }
        closeReader()
      }

      def getReader: CHStreamReader = {
        if (reader == null) {
          reader = new CHStreamReader(
            original_in,
            forceCompress,
            CHBackendSettings.useCustomizedShuffleCodec
          )
        }
        reader
      }

      def closeReader(): Unit = {
        if (reader != null) {
          reader.close()
          reader = null
        }
      }
    }
  }

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] =
      new Array[Byte](CHBackendSettings.customizeBufferSize)
    private[this] val dOut: BlockOutputStream =
      new BlockOutputStream(
        out,
        writeBuffer,
        dataSize,
        CHBackendSettings.useCustomizedShuffleCodec,
        capitalizedCompressionCodec,
        compressionLevel,
        CHBackendSettings.customizeBufferSize
      )

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val cb = value.asInstanceOf[ColumnarBatch]

      // Use for reading bytes array from block
      dOut.write(cb)
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
      writeBuffer = null
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

}
