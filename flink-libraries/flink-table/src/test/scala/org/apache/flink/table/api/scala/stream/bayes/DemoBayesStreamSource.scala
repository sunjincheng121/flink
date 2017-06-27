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
package org.apache.flink.table.api.scala.stream.bayes

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

class DemoBayesStreamSource extends AbstractStreamSource {
  override def genSourceFunction: SourceFunction[Row] = {
    val datas: List[Row] = List(
      Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
      Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
      Row.of(JLong.valueOf(4L), JInt.valueOf(3), "Hello"),
      Row.of(JLong.valueOf(8L), JInt.valueOf(4), "Hello world"),
      Row.of(JLong.valueOf(16L), JInt.valueOf(5), "Hello world"))

    new SourceFunction[Row] {
      override def run(ctx: SourceContext[Row]): Unit = {
        datas.foreach(ctx.collect)
      }
      override def cancel(): Unit = ???
    }
  }

  override def getReturnType: TypeInformation[Row] = {
    new RowTypeInfo(
      Array(Types.LONG, Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
      Array("ts", "id", "name"))
  }
}
