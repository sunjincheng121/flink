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

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions
import java.util.{ArrayList => JArrayList}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable

import org.apache.flink.api.common.typeinfo.TypeInformation

class UnboundedNonPartitionedProcessingOverResultMapFunction(
  private val aggregates: Array[AggregateFunction[_]],
  private val forwardedFieldCount: Int,
  @transient private val returnType: TypeInformation[Row])
  extends RichMapFunction[Row, Row] with ResultTypeQueryable[Row]{

  Preconditions.checkNotNull(aggregates)

  private var output: Row = _

  val accumulatorList: Array[JArrayList[Accumulator]] = Array.fill(aggregates.length) {
    new JArrayList[Accumulator](2)
  }
  override def open(config: Configuration) {
    output = new Row(forwardedFieldCount + aggregates.length)
    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
    }
  }

  override def map(record: Row): Row = {
    // reset first accumulator in merge list
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).set(0, accumulator)
    }

    for (i <- aggregates.indices) {
      // insert received accumulator into acc list
      val newAcc = record.getField(forwardedFieldCount + i).asInstanceOf[Accumulator]
      accumulatorList(i).set(1, newAcc)
      // merge acc list
      val retAcc = aggregates(i).merge(accumulatorList(i))
      // insert result into acc list
      accumulatorList(i).set(0, retAcc)
    }

    for (i <- 0 until forwardedFieldCount) {
      output.setField(i, record.getField(i))
    }

    for (i <- aggregates.indices) {
      output.setField(forwardedFieldCount + i,aggregates(i).getValue(accumulatorList(i).get(0)))
    }
    output
  }
  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
