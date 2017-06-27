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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.bayes.BayesStreamSourceITCase.BayesGenTsAndWatermark
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable

class BayesStreamSourceITCase extends StreamingWithStateTestBase {

  @Test
  def testBayesStreamSource(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    // 创建source
    val source = new DemoBayesStreamSource

    tEnv.registerTableSource("bayes", source)

    val config1 = QueryWaterMarkConfig(
      "rowtime", 0,
      // 用户可以自定义生成逻辑
      new BayesGenTsAndWatermark(Array(0, 1)))

    val config2 = QueryWaterMarkConfig(
      "rowtime2", 0,
      // 用户可以自定义生成逻辑
      new GenTsAndWatermark {
        override def extractTs(row: Row, previousElementTimestamp: Long) = {
          row.getField(0).asInstanceOf[Long]
        }

        override def genWatermark(row: Row, extractedTimestamp: Long): Long = extractedTimestamp

      })

    source.addQueryWaterMarkConfig(config1)
    source.addQueryWaterMarkConfig(config2)

    source.setCurrentQueryWaterMarkConfigName(config1.columnName)

    val sqlQuery = "SELECT " +
      "id, name, rowtime," +
      "count(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding), " +
      "sum(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) " +
      "from bayes"

    val tab = tEnv.sql(sqlQuery)

    val result = tab.toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    for (data <- StreamITCase.testResults.sorted) {
      println("\"" + data + "\",")
    }

    val expected = mutable.MutableList(
      "1,Hi,1970-01-01 00:00:00.001,1,1",
      "2,Hello,1970-01-01 00:00:00.002,1,2",
      "3,Hello,1970-01-01 00:00:00.004,2,5",
      "4,Hello world,1970-01-01 00:00:00.008,1,4",
      "5,Hello world,1970-01-01 00:00:00.016,2,9")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object BayesStreamSourceITCase {

  /**
    * 这是用自定义的类，可以任意参数和实现逻辑
    */
  class BayesGenTsAndWatermark(indexs: Array[Int]) extends GenTsAndWatermark {
    override def extractTs(row: Row, previousElementTimestamp: Long): Long = {
      row.getField(indexs(0)).asInstanceOf[Long]
    }

    override def genWatermark(row: Row, extractedTimestamp: Long): Long = {
      extractedTimestamp
    }
  }

}

