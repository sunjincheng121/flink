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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
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
import org.apache.flink.core.memory.{DataInputViewStreamWrapper, DataOutputViewStreamWrapper}
import org.apache.flink.api.table.typeutils.RowSerializer
import org.apache.flink.api.common.state.MapStateDescriptor

import scala.collection.JavaConverters._

class JoinMapStateDescriptor[K, V](
  name: String,
  keyTypeClass: Class[K],
  valueTypeClass: Class[V],
  defaultValue: V) extends MapStateDescriptor(name, keyTypeClass, valueTypeClass, defaultValue)

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

  override def hashCode(): Int = util.Arrays.hashCode(rawBytes)

  def canEqual(other: Any) = other.isInstanceOf[HashWrapper[T]]

  override def equals(other: Any) = other match {
    case that: HashWrapper[T] =>
      that.canEqual(this) && util.Arrays.equals(this.rawBytes, that.rawBytes)
    case _ => false
  }
}

class StreamConnectCoMapFunctionWithMap[L, R, O](
  joiner: RichFlatJoinFunction[L, R, O],
  leftKeys: Array[Int],
  rightKeys: Array[Int],
  leftType: CompositeType[L],
  rightType: CompositeType[R],
  resultType: TypeInformation[O]) extends
  RichCoFlatMapFunction[L, R, O] with ResultTypeQueryable[O] {

  protected val STATE_VALUE: Byte = 'v';
  protected var leftSerializer: TypeSerializer[L] = null;
  protected var rightSerializer: TypeSerializer[R] = null;
  protected var leftStateDescriptor: MapStateDescriptor[HashWrapper[L], Byte] = null
  protected var rightStateDescriptor: MapStateDescriptor[HashWrapper[R], Byte] = null

  override def open(parameters: Configuration): Unit = {
    leftSerializer = leftType.createSerializer(getRuntimeContext.getExecutionConfig)
    rightSerializer = rightType.createSerializer(getRuntimeContext.getExecutionConfig)

    leftStateDescriptor =
      new JoinMapStateDescriptor[HashWrapper[L], Byte](
        "left",
        classOf[HashWrapper[L]],
        classOf[Byte], 0)

    rightStateDescriptor =
      new JoinMapStateDescriptor[HashWrapper[R], Byte](
        "right",
        classOf[HashWrapper[R]],
        classOf[Byte],
        0)

    joiner.setRuntimeContext(getRuntimeContext)
    joiner.open(parameters)
  }

  override def flatMap1(value: L, out: Collector[O]): Unit = {
    val serializedValue = new HashWrapper[L](value, leftSerializer)
    val leftState = getRuntimeContext.getMapState(leftStateDescriptor)
    val rightState = getRuntimeContext.getMapState(rightStateDescriptor)
    val left = value.asInstanceOf[Row]
    val rowKey = new StringBuilder

    for (i <- leftKeys)
      rowKey.append(left.productElement(i))

    leftState.put(serializedValue, STATE_VALUE)
    rightState.keys().asScala.map(
      r => {
        val bais: ByteArrayInputStream = new ByteArrayInputStream(r.rawBytes)
        val in: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(bais)
        joiner
        .join(
          value,
          rightSerializer.asInstanceOf[RowSerializer].deserializeField(in).asInstanceOf[R],
          out)
      })

  }

  override def flatMap2(value: R, out: Collector[O]): Unit = {

    val serializedValue = new HashWrapper[R](value, rightSerializer)
    val leftState = getRuntimeContext.getMapState(leftStateDescriptor)
    val rightState = getRuntimeContext.getMapState(rightStateDescriptor)

    val right = value.asInstanceOf[Row]
    val rowKey = new StringBuilder
    for (i <- rightKeys)
      rowKey.append(right.productElement(i))

    rightState.put(serializedValue, STATE_VALUE)

    leftState.keys().asScala.map(
      l => {
        val bais: ByteArrayInputStream = new ByteArrayInputStream(l.rawBytes)
        val in: DataInputViewStreamWrapper = new DataInputViewStreamWrapper(bais)
        joiner
        .join(
          leftSerializer.asInstanceOf[RowSerializer].deserializeField(in).asInstanceOf[L],
          value,
          out)
        1
      })
  }

  override def getProducedType: TypeInformation[O] = resultType
}
