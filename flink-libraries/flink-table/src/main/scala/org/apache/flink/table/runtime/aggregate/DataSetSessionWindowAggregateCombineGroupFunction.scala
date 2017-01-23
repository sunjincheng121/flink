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

import org.apache.flink.api.common.functions.RichGroupCombineFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, Preconditions}


/**
  * This wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * @param aggregates The aggregate functions.
  * @param groupingKeys The indexes of the grouping fields.
  * @param intermediateRowArity The intermediate row field count.
  * @param gap  Session time window gap.
  * @param intermediateRowType Intermediate row data type.
  */
class DataSetSessionWindowAggregateCombineGroupFunction(
    aggregates: Array[Aggregate[_ <: Any]],
    groupingKeys: Array[Int],
    intermediateRowArity: Int,
    gap: Long,
    @transient intermediateRowType: TypeInformation[Row])
  extends RichGroupCombineFunction[Row,Row]
    with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var rowTimeFieldPos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupingKeys)
    aggregateBuffer = new Row(intermediateRowArity)
    rowTimeFieldPos = intermediateRowArity - 2
  }

  /**
    * For sub-grouped intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Sub-grouped intermediate aggregate Rows.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {
    DataSetSessionWindowAggregateUtil.preProcessing(
      aggregates,
      groupingKeys,
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
