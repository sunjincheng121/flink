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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.table.AggregationsITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.table.api.scala.stream.table.AggregationsITCase.{MyUdtf,MyUdtf2}
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import java.util.{ArrayList => JList}
import java.util.{Iterator => JIterator}

import scala.collection.mutable

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class AggregationsITCase extends StreamingMultipleProgramsTestBase {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  import org.apache.flink.table.functions.TableFunction

  @Test
  def testUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val udtf = new MyUdtf
    val udtf2 = new MyUdtf2
    val windowedTable = table
      .join(udtf('string) as ('name))
      .join(udtf2('name) as ('last))
      .select('last)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    for (data <- StreamITCase.testResults.sorted){
      println(data)
    }

  }



  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestdata = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(sessionWindowTestdata)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'int.sum)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("Hello World,1,9", "Hello,1,16", "Hello,4,15")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 2.rows as 'w)
      .groupBy('w)
      .select('int.count)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("2", "2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string)

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'int.avg, 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end)

    val results = windowedTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "Hello world,1,3,3,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01",
      "Hello world,1,3,3,3,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02",
      "Hello,2,2,2,2,4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,1,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object AggregationsITCase {
  class TimestampAndWatermarkWithOffset(
    offset: Long) extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
      : Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }


  class MyUdtf extends TableFunction[String] {

    def eval(data: String): java.util.List[String] = {
      val results = new java.util.ArrayList[String]()
      results.add(data+"AAAAAAAAA")
      // return java.lang.Iterable
      results
    }

    def eval(data: Int): scala.collection.immutable.List[String] = {
      // return scala.collection.Iterable
      scala.collection.immutable.List[String](String.valueOf(data))
    }

  }

  class MyUdtf2 extends TableFunction[String] {

        def eval(data: String): List[String] = {
          List[String](data, data+"hahahahahhahah")
        }
  }
}
