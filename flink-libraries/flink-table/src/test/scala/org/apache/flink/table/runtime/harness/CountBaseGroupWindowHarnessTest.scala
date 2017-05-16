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

import java.lang.{Integer => JInt}
import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.functions.windowing._
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
import org.apache.flink.streaming.runtime.operators.windowing.functions._
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks._
import org.apache.flink.streaming.util._
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.runtime.harness.CountBaseGroupWindowHarnessTest._
import org.apache.flink.table.runtime.triggers.CountTriggerWithCleanupState
import org.junit.Test
import org.apache.flink.api.java.typeutils.TypeInfoParser

class CountBaseGroupWindowHarnessTest extends HarnessTestBase {
  @Test
  @throws[Exception]
  def testCountTrigger() {
    val queryConfig =
    new StreamQueryConfig().withIdleStateRetentionTime(Time.seconds(2), Time.seconds(3))
    val WINDOW_SIZE = 4

    val inputType: TypeInformation[Tuple2[String, Integer]] =
      TypeInfoParser.parse("Tuple2<String, Integer>")

    val stateDesc: ReducingStateDescriptor[Tuple2[String, Integer]] =
      new ReducingStateDescriptor(
        "window-contents",
        new SumReducer(),
        inputType.createSerializer(new ExecutionConfig()));

    val operator: WindowOperator[String, Tuple2[String, Integer], Tuple2[String, Integer],
      Tuple2[String,
        Integer], GlobalWindow] =
      new WindowOperator[
        String,
        Tuple2[String, Integer],
        Tuple2[String, Integer],
        Tuple2[String,
          Integer],
        GlobalWindow](
        GlobalWindows.create(),
        new GlobalWindow.Serializer(),
        new TupleKeySelector(),
        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
        stateDesc,
        new InternalSingleValueWindowFunction
            (new PassThroughWindowFunction[String, GlobalWindow, Tuple2[String, Integer]]()),
        PurgingTrigger.of(CountTriggerWithCleanupState.of[GlobalWindow](queryConfig, 3)),
        0,
        null /* late data output tag */);

    val testHarness: OneInputStreamOperatorTestHarness[
      Tuple2[String, Integer],
      Tuple2[String, Integer]] =
      new KeyedOneInputStreamOperatorTestHarness(
        operator,
        new TupleKeySelector(),
        BasicTypeInfo.STRING_TYPE_INFO);

    val expectedOutput: ConcurrentLinkedQueue[Object] = new ConcurrentLinkedQueue[Object]();

    testHarness.open();

    // The global window actually ignores these timestamps...

    // add elements out-of-order
    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key2", 1), 3000));
    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key2", 1), 3999));

    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key1", 1), 20));
    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key1", 1), 0));
    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key1", 1), 999));

    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key2", 1), 1998));
    testHarness.processElement(new StreamRecord(new Tuple2[String, Integer]("key2", 1), 1999));

    // do a snapshot, close and restore again
    val snapshot: OperatorStateHandles = testHarness.snapshot(0L, 0L);



    val outputBeforeClose: ConcurrentLinkedQueue[Object] = testHarness.getOutput()

    testHarness.close();
  }
}

object CountBaseGroupWindowHarnessTest {

  class SumReducer extends ReduceFunction[Tuple2[String, Integer]] {
    override def reduce(
        value1: Tuple2[String, JInt],
        value2: Tuple2[String, JInt]): Tuple2[String, JInt] =
      new Tuple2[String, Integer](value2.f0, (value1.f1 + value2.f1))
  }

  class TupleKeySelector extends KeySelector[Tuple2[String, Integer], String] {
    override def getKey(value: Tuple2[String, JInt]): String = value.f0
  }

}
