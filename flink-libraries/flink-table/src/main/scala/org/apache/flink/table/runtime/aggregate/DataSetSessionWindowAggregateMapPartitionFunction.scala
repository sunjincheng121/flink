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
import java.lang.Iterable

import org.apache.flink.api.common.functions.RichMapPartitionFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

class DataSetSessionWindowAggregateMapPartitionFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowArity: Int,
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    gap: Long,
    isInputCombined: Boolean)
  extends RichMapPartitionFunction[Row, Row] {

  private var aggregateBuffer: Row = _
  private var intermediateRowWindowStartPos = 0
  private var intermediateRowWindowEndPos = 0
  private var output: Row = _
  private var collector: TimeWindowPropertyCollector = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    aggregateBuffer = new Row(intermediateRowArity)
    intermediateRowWindowStartPos = intermediateRowArity - 2
    intermediateRowWindowEndPos = intermediateRowArity - 1
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(finalRowWindowStartPos, finalRowWindowEndPos)
  }

  /**
    * Divide window according to the window-start
    * and window-end, merge data (within a unified window) into an aggregate buffer, calculate
    * aggregated values output from aggregate buffer, and then set them into output
    * Row based on the mapping relationship between intermediate aggregate data and output data.
    *
    * @param records  Aggregate Rows iterator.
    * @param out The collector to hand results to.
    *
    */
  override def mapPartition(
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    DataSetSessionWindowAggregateUtil.processing(
      aggregates,
      Array(),
      aggregateMapping,
      intermediateRowWindowStartPos,
      intermediateRowWindowEndPos,
      output,
      aggregateBuffer,
      gap,
      isInputCombined,
      collector,
      finalRowWindowStartPos,
      finalRowWindowEndPos,
      records,
      out
    )
  }
}
