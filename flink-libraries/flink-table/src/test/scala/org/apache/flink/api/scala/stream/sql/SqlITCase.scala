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

package org.apache.flink.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.{StreamITCase, StreamTestData}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils.{TableValuedFunction0,TableValuedFunction1}
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SqlITCase extends StreamingMultipleProgramsTestBase {

  /** test selection **/
  @Test
  def testSelectExpressionFromTable(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT a * 2, b - 1 FROM MyTable"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("2,0", "4,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered table **/
  @Test
  def testSimpleFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE a = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test filtering with registered datastream **/
  @Test
  def testDatastreamFilter(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM MyTable WHERE _1 = 3"

    val t = StreamTestData.getSmall3TupleDataStream(env)
    tEnv.registerDataStream("MyTable", t)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with registered tables **/
  @Test
  def testUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 " +
      "UNION ALL " +
      "SELECT * FROM T2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi", "1,1,Hi",
      "2,2,Hello", "2,2,Hello",
      "3,2,Hello world", "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union with filter **/
  @Test
  def testUnionWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT * FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT * FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T2", t2)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  /** test union of a table and a datastream **/
  @Test
  def testUnionTableWithDataSet(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val sqlQuery = "SELECT c FROM T1 WHERE a = 3 " +
      "UNION ALL " +
      "SELECT c FROM T2 WHERE a = 2"

    val t1 = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("T1", t1)
    val t2 = StreamTestData.get3TupleDataStream(env)
    tEnv.registerDataStream("T2", t2, 'a, 'b, 'c)

    val result = tEnv.sql(sqlQuery).toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("split", new TableValuedFunction0())
    val sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s " +
      "FROM MyTable,LATERAL TABLE(split(c)) AS t(s)"
    val tab = tEnv.sql(sqlQuery)
    val result = tab.toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1,6,Hi",
      "1,6,KEVIN", "2,7,Hello", "2,7,SUNNY", "4,8,LOVER", "4,8,PAN")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUDTFWithFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("split", new TableValuedFunction0())
    val sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s " +
      "FROM MyTable,LATERAL TABLE(split(c)) AS t(s)" +
    "WHERE MyTable.a < 4"
    val tab = tEnv.sql(sqlQuery)
    val result = tab.toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1,6,Hi",
      "1,6,KEVIN", "2,7,Hello", "2,7,SUNNY")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testLeftJoinUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = getSmall3TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("split", new TableValuedFunction0())
    val sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s " +
      "FROM MyTable LEFT JOIN LATERAL TABLE(split(c)) AS t(s) ON TRUE"
    val tab = tEnv.sql(sqlQuery)
    val result = tab.toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("1,6,Hi",
      "1,6,KEVIN", "2,7,Hello", "2,7,SUNNY","3,7,null", "4,8,LOVER", "4,8,PAN")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInnerJoinUDTF(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = getSmall4TupleDataStream(env).toTable(tEnv).as('a, 'b, 'c,'d)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("split", new TableValuedFunction0())
    val sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.s " +
      "FROM MyTable JOIN LATERAL TABLE(split(c)) AS t(s) ON MyTable.d=t.s"
    val tab = tEnv.sql(sqlQuery)
    val result = tab.toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,6,KEVIN","2,7,SUNNY")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInnerJoinUDTF2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t = getSmall3TupleDataStream2(env).toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTable", t)
    tEnv.registerFunction("split", new TableValuedFunction1())
    val sqlQuery = "SELECT MyTable.a, MyTable.b+5, t.age,t.name " +
      "FROM MyTable JOIN LATERAL TABLE(split(c)) AS t(age,name) ON MyTable.a=t.age"
    val tab = tEnv.sql(sqlQuery)
    val result = tab.toDataStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,6,1,KEVIN","2,7,2,SUNNY")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  def getSmall3TupleDataStream(env:
                               StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi#KEVIN"))
    data.+=((2, 2L, "Hello#SUNNY"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "PAN#LOVER"))
    env.fromCollection(data)
  }
  def getSmall3TupleDataStream2(env:
                               StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "1#KEVIN"))
    data.+=((2, 2L, "2#SUNNY"))
    data.+=((3, 2L, "Hello world"))
    data.+=((4, 3L, "20#LOVER"))
    env.fromCollection(data)
  }
  def getSmall4TupleDataStream(env: StreamExecutionEnvironment):
  DataStream[(Int, Long, String,String)] = {
    val data = new mutable.MutableList[(Int, Long, String,String)]
    data.+=((1, 1L, "Hi#KEVIN","KEVIN"))
    data.+=((2, 2L, "Hello#SUNNY","SUNNY"))
    data.+=((3, 2L, "Hello#world","a"))
    data.+=((4, 3L, "PAN#LOVER","a"))
    env.fromCollection(data)
  }
}


