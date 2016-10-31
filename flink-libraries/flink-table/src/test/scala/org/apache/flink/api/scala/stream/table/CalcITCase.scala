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

package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.api.table.expressions.utils.TableValuedFunction0
import org.apache.flink.api.table.{Row, TableEnvironment, TableException}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class CalcITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1, '_2, '_3)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSelectFirst(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {

    // verify ProjectMergeRule.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "3,2", "4,3", "5,3", "6,3", "7,4",
      "8,4", "9,4", "10,4", "11,5", "12,5", "13,5", "14,5", "15,5",
      "16,6", "17,6", "18,6", "19,6", "20,6", "21,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testAsWithToFewFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testAsWithToManyFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testAsWithAmbiguousFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'b)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test(expected = classOf[TableException])
  def testOnlyFieldRefInAs(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b as 'c, 'd)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleFilter(): Unit = {
    /*
     * Test simple filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter('a === 3)
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    /*
     * Test all-rejecting filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    /*
     * Test all-passing filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(true) )
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello", "4,3,Hello world, how are you?",
      "6,3,Luke Skywalker", "8,4,Comment#2", "10,4,Comment#4",
      "12,5,Comment#6", "14,5,Comment#8", "16,6,Comment#10",
      "18,6,Comment#12", "20,6,Comment#14")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testNotEquals(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = mutable.MutableList(
      "1,1,Hi", "3,2,Hello world",
      "5,3,I am fine.", "7,4,Comment#1", "9,4,Comment#3",
      "11,5,Comment#5", "13,5,Comment#7", "15,5,Comment#9",
      "17,6,Comment#11", "19,6,Comment#13", "21,6,Comment#15")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
  @Test
  def testUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val t = getSmall4TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)

    val udtf = new TableValuedFunction0
    val result = t.join(udtf('c) as ('w))
      .select('a, 'b + 5, 'w)
      .toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = mutable.MutableList("1,6,Hi", "1,6,KEVIN",
      "2,7,Hello", "2,7,SUNNY", "4,8,LOVER", "4,8,PAN")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
  @Test
  def testLeftUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    val t = getSmall4TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)

    val udtf = new TableValuedFunction0
    val result = t.leftJoin(udtf('c) as ('w))
      .select('a, 'b + 5, 'w)
      .toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = mutable.MutableList("1,6,Hi", "1,6,KEVIN",
      "2,7,Hello", "2,7,SUNNY","3,7,null","4,8,LOVER", "4,8,PAN")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
  def getSmall4TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi#KEVIN"))
    data.+=((2, 2L, "Hello#SUNNY"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "PAN#LOVER"))
    env.fromCollection(data)
  }
}
