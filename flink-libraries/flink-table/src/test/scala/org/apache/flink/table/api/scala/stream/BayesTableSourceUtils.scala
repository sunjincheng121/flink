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
package org.apache.flink.table.api.scala.stream

import java.lang.{Integer => JInt, Long => JLong}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.Types
import org.apache.flink.table.sources.{DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.types.Row

object BayesTableSourceUtils {
  /**
    * Bayes table StreamTableSource 示例
    * @param timeField 用于作为eventTime的列名
    * @param watermarkWithOffset 用于定制watermark，相当于DataStream的allowlateness
    */
  class BayesStreamSourceUsingSourceFunction(
      timeField: String,
      watermarkWithOffset: Long)
    extends StreamTableSource[Row] with DefinedRowtimeAttribute {

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      val datas: List[Row] = List(
        Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
        Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
        Row.of(JLong.valueOf(4L), JInt.valueOf(2), "Hello"),
        Row.of(JLong.valueOf(8L), JInt.valueOf(3), "Hello world"),
        Row.of(JLong.valueOf(16L), JInt.valueOf(3), "Hello world"))
      val timeFileIndex = getReturnType.asInstanceOf[RowTypeInfo].getFieldIndex(timeField)

      var dataWithTsAndWatermark: Seq[Either[(Long, Row), Long]] = Seq[Either[(Long, Row), Long]]()
      datas.foreach {
        data =>
          val left = Left(data.getField(timeFileIndex).asInstanceOf[Long], data)
          val right = Right(data.getField(timeFileIndex).asInstanceOf[Long] - watermarkWithOffset)
          dataWithTsAndWatermark = dataWithTsAndWatermark ++ Seq(left) ++ Seq(right)
      }

      execEnv
      .addSource(new BayesEventTimeSourceFunction(dataWithTsAndWatermark))
      .returns(getReturnType)
    }

    override def getRowtimeAttribute: String = "rowtime"

    override def getReturnType: TypeInformation[Row] = {
      new RowTypeInfo(
        Array(Types.LONG, Types.INT, Types.STRING)
        .asInstanceOf[Array[TypeInformation[_]]],
        Array("ts", "id", "name"))
    }
  }

  /**
    * Bayes table StreamTableSource 示例
    *
    * @param timeField 用于作为eventTime的列名
    * @param watermarkWithOffset 用于定制watermark，相当于DataStream的allowlateness
    */
  class BayesStreamSourceUsingTimestampAndWatermarkAssigner(
      timeField: String,
      watermarkWithOffset:Long)
    extends StreamTableSource[Row] with DefinedRowtimeAttribute {

    override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
      val datas: List[Row] = List(
        Row.of(JLong.valueOf(1L), JInt.valueOf(1), "Hi"),
        Row.of(JLong.valueOf(2L), JInt.valueOf(2), "Hello"),
        Row.of(JLong.valueOf(4L), JInt.valueOf(2), "Hello"),
        Row.of(JLong.valueOf(8L), JInt.valueOf(3), "Hello world"),
        Row.of(JLong.valueOf(16L), JInt.valueOf(3), "Hello world"))

      execEnv
      .addSource(new BayesSourceFunction(datas))
      .assignTimestampsAndWatermarks(
        new BayesTimestampAndWatermarkWithOffset(
          watermarkWithOffset,
          getReturnType.asInstanceOf[RowTypeInfo].getFieldIndex(timeField)))
      .returns(getReturnType)
    }

    override def getRowtimeAttribute: String = "rowtime"

    override def getReturnType: TypeInformation[Row] = {
      new RowTypeInfo(
        Array(Types.LONG, Types.INT, Types.STRING).asInstanceOf[Array[TypeInformation[_]]],
        Array("ts","id", "name"))
    }
  }

  /**
    * Bayes source function 示例，提取数据,并携带的timestamp和watermark
    *
    * @param dataWithTimestampList 携带timestamp和watermark信息的数据列表
    */
  class BayesEventTimeSourceFunction(
      dataWithTimestampList: Seq[Either[(Long, Row), Long]]) extends SourceFunction[Row] {
    override def run(ctx: SourceContext[Row]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = ???
  }

  /**
    * Bayes source function 示例，提取数据
    *
    * @param dataList 数据列表
    */
  class BayesSourceFunction(
      dataList: Seq[Row]) extends SourceFunction[Row] {
    override def run(ctx: SourceContext[Row]): Unit = {
      dataList.foreach(ctx.collect)
    }

    override def cancel(): Unit = ???
  }

  /**
    * Bayes 默认 timestamp和watermark 提取生成器 示例
    *
    * @param offset
    * @param timeStampIndex
    */
  class BayesTimestampAndWatermarkWithOffset(
      offset: Long, timeStampIndex: Int) extends AssignerWithPunctuatedWatermarks[Row] {

    override def checkAndGetNextWatermark(
        lastElement: Row,
        extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(element: Row, previousElementTimestamp: Long): Long = {
      element.getField(timeStampIndex).asInstanceOf[Long]
    }
  }
}
