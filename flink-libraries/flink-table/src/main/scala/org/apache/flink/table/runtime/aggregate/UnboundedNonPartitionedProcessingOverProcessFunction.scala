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
package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class UnboundedNonPartitionedProcessingOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val aggregationStateType: RowTypeInfo,
    private val returnType: TypeInformation[Row])
  extends ProcessFunction[Row, Row] with CheckpointedFunction{

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var accumulators: Row = _
  private var output: Row = _
  private var accumulatorsState:ListState[Row] = null


  private val aggregateWithIndex: Array[(AggregateFunction[_], Int)] = aggregates.zipWithIndex

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    accumulators = new Row(aggregates.length)
    aggregateWithIndex.foreach { case (agg, i) =>
      accumulators.setField(i, agg.createAccumulator())
    }
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    for (i <- 0 until forwardedFieldCount) {
      output.setField(i, input.getField(i))
    }

    for (i <- aggregates.indices) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
      output.setField(index, aggregates(i).getValue(accumulator))
    }
    accumulatorsState.clear()
    accumulatorsState.add(accumulators)

    out.collect(output)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    accumulatorsState.add(accumulators)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

    val accumulatorsSerializer =
      aggregationStateType.createSerializer(getRuntimeContext.getExecutionConfig)
    val accumulatorsDescriptor = new ListStateDescriptor[Row]("overState",accumulatorsSerializer)
    accumulatorsState =
      context.getOperatorStateStore.getOperatorState(accumulatorsDescriptor)
  }
}
