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
package org.apache.gluten.ras.util

import scala.collection.mutable

trait IndexDisjointSet {
  def grow(): Unit
  def forward(from: Int, to: Int): Unit
  def find(ele: Int): Int
  def setOf(ele: Int): Set[Int]
  def size(): Int
}

object IndexDisjointSet {
  def apply[T <: Any](): IndexDisjointSet = new IndexDisjointSetImpl()

  private class IndexDisjointSetImpl extends IndexDisjointSet {
    import IndexDisjointSetImpl._

    private val nodeBuffer: mutable.ArrayBuffer[Node] = mutable.ArrayBuffer()

    override def grow(): Unit = nodeBuffer += new Node(nodeBuffer.size)

    override def forward(from: Int, to: Int): Unit = {
      if (from == to) {
        // Already in one set.
        return
      }

      val fromNode = nodeBuffer(from)
      val toNode = nodeBuffer(to)
      assert(fromNode.parent.isEmpty, "Only root element is allowed to forward")
      assert(toNode.parent.isEmpty, "Only root element is allowed to forward")

      fromNode.parent = Some(to)
      toNode.children += from
    }

    private def find0(ele: Int): Node = {
      var cursor = nodeBuffer(ele)
      while (cursor.parent.nonEmpty) {
        cursor = nodeBuffer(cursor.parent.get)
      }
      cursor
    }

    override def find(ele: Int): Int = {
      find0(ele).index
    }

    override def setOf(ele: Int): Set[Int] = {
      val rootNode = find0(ele)
      val buffer = mutable.ListBuffer[Int]()
      dfsAdd(rootNode, buffer)
      buffer.toSet
    }

    private def dfsAdd(node: Node, buffer: mutable.ListBuffer[Int]): Unit = {
      buffer += node.index
      node.children.foreach(child => dfsAdd(nodeBuffer(child), buffer))
    }

    override def size(): Int = {
      nodeBuffer.size
    }

    private def checkBound(ele: Int): Unit = {
      assert(ele < nodeBuffer.size, "Grow the disjoint set first")
    }
  }

  private object IndexDisjointSetImpl {
    private class Node(val index: Int) {
      var parent: Option[Int] = None
      val children: mutable.ListBuffer[Int] = mutable.ListBuffer()
    }
  }
}
