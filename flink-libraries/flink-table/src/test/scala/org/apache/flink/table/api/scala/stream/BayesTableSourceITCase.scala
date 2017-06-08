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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.BayesTableSourceUtils._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

import scala.collection.mutable


class BayesTableSourceITCase extends StreamingWithStateTestBase {

  @Test
  def testBayesStreamSourceUsingSourceFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    tEnv.registerTableSource("bayes", new BayesStreamSourceUsingSourceFunction("ts", 0L))

    val sqlQuery = "SELECT " +
      "id, name, rowtime," +
      "count(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding), " +
      "sum(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) " +
      "from bayes"

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hi,1970-01-01 00:00:00.001,1,1",
      "2,Hello,1970-01-01 00:00:00.002,1,2",
      "2,Hello,1970-01-01 00:00:00.004,2,4",
      "3,Hello world,1970-01-01 00:00:00.008,1,3",
      "3,Hello world,1970-01-01 00:00:00.016,2,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testBayesStreamSourceUsingTimestampAndWatermarkAssigner(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear
    tEnv.registerTableSource(
      "bayes",
      new BayesStreamSourceUsingTimestampAndWatermarkAssigner("ts", 0L))

    val sqlQuery = "SELECT " +
      "id, name, rowtime," +
      "count(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding), " +
      "sum(id) OVER (PARTITION BY name ORDER BY rowtime RANGE UNBOUNDED preceding) " +
      "from bayes"

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Hi,1970-01-01 00:00:00.001,1,1",
      "2,Hello,1970-01-01 00:00:00.002,1,2",
      "2,Hello,1970-01-01 00:00:00.004,2,4",
      "3,Hello world,1970-01-01 00:00:00.008,1,3",
      "3,Hello world,1970-01-01 00:00:00.016,2,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

