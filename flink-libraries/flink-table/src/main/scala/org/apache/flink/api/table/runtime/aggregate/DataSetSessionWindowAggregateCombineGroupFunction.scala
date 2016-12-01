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
package org.apache.flink.api.table.runtime.aggregate

import java.lang.Iterable

import org.apache.flink.api.common.functions.RichGroupCombineFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.api.table.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.{Collector, Preconditions}

import scala.collection.JavaConversions._


class DataSetSessionWindowAggregateCombineGroupFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupingKeys: Array[Int],
    private var intermediateRowArity: Int,
    private val gap:Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichGroupCombineFunction[Row,Row]
  with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var intermediateRowWindowStartPos = 0
  private var intermediateRowWindowEndPos = 0

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupingKeys)
    aggregateBuffer = new Row(intermediateRowArity)
    intermediateRowWindowStartPos = intermediateRowArity - 2
    intermediateRowWindowEndPos = intermediateRowArity - 1
  }

  override def combine(
    records: Iterable[Row],
    out: Collector[Row]): Unit = {
    // Merge intermediate aggregate value to buffer.
    var last:Row = null
    var head:Row = null
    var lastWindowEnd: Long = -1
    var currentWindowStart: Long = -1

    records.foreach(
      (record) => {
        currentWindowStart =
          record.productElement(intermediateRowWindowStartPos).asInstanceOf[Long]

        if (lastWindowEnd == -1) {
          // Initiate intermediate aggregate value.
          aggregates.foreach(_.initiate(aggregateBuffer))
          head = record
          lastWindowEnd = currentWindowStart
        }

        if (currentWindowStart > lastWindowEnd) {
          doCollect(out, last, head)
          head = record
          // Initiate intermediate aggregate value.
          aggregates.foreach(_.initiate(aggregateBuffer))
        }
        aggregates.foreach(_.merge(record, aggregateBuffer))
        last = record
        lastWindowEnd =
          record.productElement(intermediateRowWindowStartPos).asInstanceOf[Long] + gap
      })

    doCollect(out, last, head)

  }

  def doCollect(
    out: Collector[Row],
    last: Row,
    head: Row): Unit =
  {
    // Set group keys to aggregateBuffer.
    for (i <- 0 until groupingKeys.length) {
      aggregateBuffer.setField(i, last.productElement(i))
    }

    val start = head.productElement(intermediateRowWindowStartPos)
                .asInstanceOf[Long]
    val end = last.productElement(intermediateRowWindowStartPos)
              .asInstanceOf[Long] + gap

    aggregateBuffer.setField(intermediateRowWindowStartPos, start)
    aggregateBuffer.setField(intermediateRowWindowEndPos, end)

    out.collect(aggregateBuffer)
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
