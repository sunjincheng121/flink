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

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

class UnboundedNonPartitionedProcessingOverResultMapFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val forwardedFieldCount: Int,
    private val returnType: TypeInformation[Row])
  extends RichMapFunction[Row, Row]
  with ResultTypeQueryable[Row]{

  Preconditions.checkNotNull(aggregates)

  private var accumulators: Row = _
  private var output: Row = _
  private val aggregateWithIndex: Array[(AggregateFunction[_], Int)] = aggregates.zipWithIndex

  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    accumulators = new Row(aggregates.length)
    aggregateWithIndex.foreach { case (agg, i) =>
      accumulators.setField(i, agg.createAccumulator())
    }
  }

  override def map(value: Row): Row = {
    for (i <- aggregates.indices) {
      // merge acc list
      val retAcc = aggregates(i).merge(
        new util.ArrayList[Accumulator]() {
          accumulators
        })
      // insert result into acc list
      accumulators.setField(forwardedFieldCount + i, retAcc)
    }

    for (i <- 0 until forwardedFieldCount) {
      output.setField(i, value.getField(i))
    }

    for (i <- aggregates.indices) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      output.setField(index, aggregates(i).getValue(accumulator))
    }
    output
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
