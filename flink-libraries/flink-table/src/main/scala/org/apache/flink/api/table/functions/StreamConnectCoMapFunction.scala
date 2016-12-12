/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.table.functions

import java.io.ByteArrayOutputStream
import java.util

import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{CompositeType, TypeSerializer}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.api.table.typeutils.RowSerializer

class HashWrapper[T](raw: T, serializer: TypeSerializer[T]) extends Serializable {
  val rawBytes = raw match {
    case row: Row =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.asInstanceOf[RowSerializer].serializeField(row, out)
      baos.toByteArray
    case _ =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.serialize(raw, out)
      baos.toByteArray
  }
  override def hashCode():Int = util.Arrays.hashCode(rawBytes)

  def canEqual(other : Any) = other.isInstanceOf[HashWrapper[T]]

  override def equals(other: Any) = other match {
    case that: HashWrapper[T] =>
      that.canEqual(this) && util.Arrays.equals(this.rawBytes, that.rawBytes)
    case _ => false
  }
}

class StreamConnectCoMapFunction[L, R, O](
  joiner: RichFlatJoinFunction[L, R, O],
  leftKeys: Array[Int],
  rightKeys: Array[Int],
  leftType: CompositeType[L],
  rightType: CompositeType[R],
  resultType: TypeInformation[O]) extends
  RichCoFlatMapFunction[L, R, O] with ResultTypeQueryable[O] {


  protected var leftSerializer: TypeSerializer[L] = null;
  protected var rightSerializer: TypeSerializer[R] = null;
  protected var leftStateDescriptor: ListStateDescriptor[L] = null
  protected var rightStateDescriptor: ListStateDescriptor[R] = null

  override def open(parameters: Configuration): Unit = {
    leftSerializer = leftType.createSerializer(getRuntimeContext.getExecutionConfig)
    rightSerializer = rightType.createSerializer(getRuntimeContext.getExecutionConfig)
    leftStateDescriptor = new ListStateDescriptor[L]("left", leftSerializer)
    rightStateDescriptor = new ListStateDescriptor[R]("right", rightSerializer)
    joiner.setRuntimeContext(getRuntimeContext)
    joiner.open(parameters)
  }

  override def flatMap1(value: L, out: Collector[O]): Unit = {
    val leftState: ListState[L] = getRuntimeContext.getListState(leftStateDescriptor)
    val rightState: ListState[R] = getRuntimeContext.getListState(rightStateDescriptor)
    val left = value.asInstanceOf[Row]
    val rowKey = new StringBuilder
    for (i <- leftKeys)
      rowKey.append(left.productElement(i))
    leftState.add(value)
    val right = rightState.get().iterator()
    while (right.hasNext) {
      joiner.join(value, right.next(), out)
    }
  }

  override def flatMap2(value: R, out: Collector[O]): Unit = {
    val leftState: ListState[L] = getRuntimeContext.getListState(leftStateDescriptor)
    val rightState: ListState[R] = getRuntimeContext.getListState(rightStateDescriptor)

    val right = value.asInstanceOf[Row]
    val rowKey = new StringBuilder
    for (i <- rightKeys)
      rowKey.append(right.productElement(i))

    rightState.add(value)

    val left = leftState.get().iterator()
    while (left.hasNext) {
      joiner.join(left.next(), value, out)
    }
  }

  override def getProducedType: TypeInformation[O] = resultType
}
