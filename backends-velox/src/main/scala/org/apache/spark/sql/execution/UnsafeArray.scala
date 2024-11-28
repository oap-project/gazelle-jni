package org.apache.spark.sql.execution

import org.apache.spark.memory.{MemoryConsumer, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData
import org.apache.spark.unsafe.{Platform, UnsafeAlignedOffset}
import org.apache.spark.unsafe.memory.MemoryBlock

class UnsafeArray(taskMemoryManager: TaskMemoryManager)  extends MemoryConsumer(taskMemoryManager, MemoryMode.OFF_HEAP) {

  protected var page: MemoryBlock = null
  acquirePage(taskMemoryManager.pageSizeBytes)
  protected var base: AnyRef = page.getBaseObject
  protected var pageCursor = 0
  private var keyOffsets: Array[Long] = null
  protected var numRows = 0

  def iterator() {}

  private def acquirePage(requiredSize: Long): Boolean = {
    try page = allocatePage(requiredSize)

    catch {
      case SparkOutOfMemoryError =>
        return false
    }
    base = page.getBaseObject
    pageCursor = 0
    true
  }

  def get(rowId: Int): UnsafeArrayData = {
    val offset = keyOffsets(rowId)
    val klen = UnsafeAlignedOffset.getSize(base, offset - UnsafeAlignedOffset.getUaoSize)
    val result = new UnsafeArrayData
    result.pointTo (base, offset, klen)
    result
  }

  def write(bytes: Array[Byte], inputOffset: Long, inputLength: Int): Unit = {
    var offset: Long = page.getBaseOffset + pageCursor
    val recordOffset = offset

    val uaoSize = UnsafeAlignedOffset.getUaoSize

    val recordLength = 2L * uaoSize + inputLength + 8L

    UnsafeAlignedOffset.putSize(base, offset, inputLength + uaoSize)
    offset += 2L * uaoSize
    Platform.copyMemory(bytes, inputOffset, base, offset, inputLength)
    Platform.putLong(base, offset, 0)

    pageCursor += recordLength
    keyOffsets(numRows) = recordOffset + 2L * uaoSize;
    numRows += 1
  }

  override def spill(l: Long, memoryConsumer: MemoryConsumer): Long = ???
}
