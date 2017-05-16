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
package org.apache.flink.table.runtime.harness

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.windowing._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.{Time => WTime}
import org.apache.flink.api.common.ExecutionConfig
import java.lang.{Integer => JInt, Long => JLong}

import com.esotericsoftware.kryo.serializers.DefaultSerializers.ByteSerializer
import org.apache.flink.streaming.runtime.operators.windowing.functions._
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.state.{ListStateDescriptor, ReducingStateDescriptor}
import org.apache.flink.streaming.runtime.operators.windowing.{EvictingWindowOperator, WindowOperator}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.TypeInfoParser
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.keys.KeySelectorUtil
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.aggregate.GroupAggProcessFunction
import org.apache.flink.table.runtime.harness.HarnessTestBase.{RowResultSortComparator, TupleRowKeySelector}
import org.apache.flink.table.runtime.triggers.CountTriggerWithCleanupState
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.junit.Test

class CountBaseGroupWindowHarnessTest extends HarnessTestBase{
  protected var queryConfig =
    new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))

  @Test
  def testProcTimeNonWindow(): Unit = {

    val processFunction = new KeyedProcessOperator[String, CRow, CRow](
      new GroupAggProcessFunction(
        genSumAggFunction,
        sumAggregationStateType,
        false,
        queryConfig))
    import org.apache.flink.streaming.api.windowing.assigners._
    import org.apache.flink.api.common.typeutils._
    import org.apache.flink.api.java.functions._
    import org.apache.flink.api.java.typeutils._
    import org.apache.flink.api.java.typeutils.runtime.RowSerializerTest
    import org.apache.flink.api.java.functions.NullByteKeySelector

    val windowAssigner: WindowAssigner[Object, GlobalWindow] = GlobalWindows.create()
    val windowSerializer: TypeSerializer[GlobalWindow] =
      windowAssigner.getWindowSerializer(new ExecutionConfig())
    val keySelector: NullByteKeySelector[Row] = new NullByteKeySelector[Row]
    val keySerializer: ByteSerializer = new ByteSerializer()
    val typeInfo = new RowTypeInfo(
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    val serializer = typeInfo.createSerializer(new ExecutionConfig)
    val stateDesc: ListStateDescriptor[Row] = new ListStateDescriptor[Row]("xx", serializer)
    val aggregateFunction = null
    val windowFunction = null
    val internalFunction = new InternalIterableAllWindowFunction(
      new AggregateApplyAllWindowFunction(aggregateFunction, windowFunction))
    val trigger =  CountTriggerWithCleanupState.of[GlobalWindow](queryConfig, 3)
    val evictor = CountEvictor.of(2)
    val allowedLateness = 0
    val lateDataOutputTag = null
    val operator = new EvictingWindowOperator(
      windowAssigner,
      windowSerializer,
      keySelector,
      keySerializer,
      stateDesc,
      internalFunction,
      trigger,
      evictor,
      allowedLateness,
      lateDataOutputTag)

    val testHarness =
      createKeyedOneInputStreamOperatorTestHarness(
        operator,
        new TupleRowKeySelector[String](0),
        BasicTypeInfo.STRING_TYPE_INFO)

    testHarness.open()

    // register cleanup timer with 3001
    testHarness.setProcessingTime(1)

    testHarness.processElement(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt, "bbb"), true), 1))
    // reuse timer 3001
    testHarness.setProcessingTime(1000)
    testHarness.processElement(new StreamRecord(CRow(Row.of(3L: JLong, 2: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(4L: JLong, 3: JInt, "aaa"), true), 1))

    // register cleanup timer with 4002
    testHarness.setProcessingTime(1002)
    testHarness.processElement(new StreamRecord(CRow(Row.of(5L: JLong, 4: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(6L: JLong, 2: JInt, "bbb"), true), 1))

    // trigger cleanup timer and register cleanup timer with 7003
    testHarness.setProcessingTime(4003)
    testHarness.processElement(new StreamRecord(CRow(Row.of(7L: JLong, 5: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(8L: JLong, 6: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(9L: JLong, 7: JInt, "aaa"), true), 1))
    testHarness.processElement(new StreamRecord(CRow(Row.of(10L: JLong, 3: JInt, "bbb"), true), 1))

    val result = testHarness.getOutput

    val expectedOutput = new ConcurrentLinkedQueue[Object]()

    expectedOutput.add(new StreamRecord(CRow(Row.of(1L: JLong, 1: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(2L: JLong, 1: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(3L: JLong, 3: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(4L: JLong, 6: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(5L: JLong, 10: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(6L: JLong, 3: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(7L: JLong, 5: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(8L: JLong, 11: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(9L: JLong, 18: JInt), true), 1))
    expectedOutput.add(new StreamRecord(CRow(Row.of(10L: JLong, 3: JInt), true), 1))

    verify(expectedOutput, result, new RowResultSortComparator(6))

    testHarness.close()
  }
}

object CountBaseGroupWindowHarnessTest {

  class SumReducer extends ReduceFunction[Tuple2[String, Integer]] {
    override def reduce(
        value1: Tuple2[String, JInt],
        value2: Tuple2[String, JInt]): Tuple2[String, JInt] =
      new Tuple2[String, Integer](value2.f0, (value1.f1 + value2.f1))
  }

}
