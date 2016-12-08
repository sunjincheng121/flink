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

import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.api.table.functions.utils.TempStore

import scala.collection.mutable.ArrayBuffer

/**
  * Created by jincheng.sunjc on 16/12/8.
  */
class StreamConnectCoMapFunction[L, R, O](
  joiner: RichFlatJoinFunction[L, R, O],
  leftKeys: Array[Int],
  rightKeys: Array[Int],
  resultType: TypeInformation[O]) extends
  RichCoFlatMapFunction[L,
    R, O] with ResultTypeQueryable[O] {

  override def open(parameters: Configuration): Unit = {
    joiner.setRuntimeContext(getRuntimeContext)
    joiner.open(parameters)
  }

  override def flatMap1(value: L, out: Collector[O]): Unit = {
    val left = value.asInstanceOf[Row]
    val rowKey = new StringBuilder
    for (i <- leftKeys)
      rowKey.append(left.productElement(i))

    TempStore.lput(rowKey.toString(), value.asInstanceOf[Row])

    val right = TempStore.rget(rowKey.toString())
    if (right.isDefined) {
      right.get.foreach(
        rdata => {
          joiner.join(value, rdata.asInstanceOf[R], out)
        })
    }
  }

  override def flatMap2(value: R, out: Collector[O]): Unit = {

    val right = value.asInstanceOf[Row]
    val rowKey = new StringBuilder
    for (i <- rightKeys)
      rowKey.append(right.productElement(i))
    TempStore.rput(rowKey.toString(), value.asInstanceOf[Row])

    val left = TempStore.lget(rowKey.toString())
    if (left.isDefined) {
      left.get.foreach(
        ldata => {
          joiner.join(ldata.asInstanceOf[L], value, out)
        })
    }

  }

  override def getProducedType: TypeInformation[O] = resultType
}
