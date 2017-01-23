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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

class DataSetSessionWindowAggregatePreMapPartitionFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    intermediateRowArity: Int,
    gap: Long,
    @transient intermediateRowType: TypeInformation[Row])
  extends RichMapPartitionFunction[Row, Row]
    with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var rowTimeFieldPos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    aggregateBuffer = new Row(intermediateRowArity)
    rowTimeFieldPos = intermediateRowArity - 2
  }

  /**
    * Divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Intermediate aggregate Rows.
    * @return Pre partition intermediate aggregate Row.
    *
    */
  override def mapPartition(
    records: Iterable[Row],
    out: Collector[Row]): Unit = {
    DataSetSessionWindowAggregateUtil.preProcessing(
      aggregates,
      Array(),
      rowTimeFieldPos,
      gap,
      aggregateBuffer,
      records,
      out)
  }

  override def getProducedType: TypeInformation[Row] = {
    intermediateRowType
  }
}
