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

import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object DataSetSessionWindowAggregateUtil {
  /**
    * Intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records Intermediate aggregate Rows.
    * @return PreProcessing intermediate aggregate Row.
    *
    */
  def preProcessing(
    aggregates: Array[Aggregate[_ <: Any]],
    groupingKeys: Array[Int],
    rowTimeFieldPos: Int,
    gap: Long,
    aggregateBuffer: Row,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (windowEnd == null || (windowEnd != null && (currentRowTime > windowEnd))) {

        // calculate the current window and open a new window.
        if (windowEnd != null) {
          // emit the current window's merged data
          doCollect(aggregateBuffer, rowTimeFieldPos, out, windowStart, windowEnd)
        } else {
          // set group keys to aggregateBuffer.
          for (i <- groupingKeys.indices) {
            aggregateBuffer.setField(i, record.getField(i))
          }
        }

        // initiate intermediate aggregate value.
        aggregates.foreach(_.initiate(aggregateBuffer))
        windowStart = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      }

      // merge intermediate aggregate value to the buffered value.
      aggregates.foreach(_.merge(record, aggregateBuffer))

      // the current rowtime is the last rowtime of the next calculation.
      windowEnd = currentRowTime + gap
    }
    // emit the merged data of the current window.
    doCollect(aggregateBuffer, rowTimeFieldPos, out, windowStart, windowEnd)
  }

  /**
    * Emit the merged data of the current window.
    *
    * @param windowStart the window's start attribute value is the min (rowtime)
    *                    of all rows in the window.
    * @param windowEnd   the window's end property value is max (rowtime) + gap
    *                    for all rows in the window.
    */
  def doCollect(
    aggregateBuffer: Row,
    rowTimeFieldPos: Int,
    out: Collector[Row],
    windowStart: Long,
    windowEnd: Long): Unit = {

    // intermediate Row WindowStartPos is rowtime pos .
    aggregateBuffer.setField(rowTimeFieldPos, windowStart)
    // intermediate Row WindowEndPos is rowtime pos + 1 .
    aggregateBuffer.setField(rowTimeFieldPos + 1, windowEnd)

    out.collect(aggregateBuffer)
  }

  /**
    * Intermediate aggregate Rows, divide window according to the window-start
    * and window-end, merge data (within a unified window) into an aggregate buffer, calculate
    * aggregated values output from aggregate buffer, and then set them into output
    * Row based on the mapping relationship between intermediate aggregate data and output data.
    *
    * @param records Intermediate aggregate Rows iterator.
    * @param out     The collector to hand results to.
    *
    */
  def processing(
    aggregates: Array[Aggregate[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowWindowStartPos: Int,
    intermediateRowWindowEndPos: Int,
    output: Row,
    aggregateBuffer: Row,
    gap:Long,
    isInputCombined: Boolean,
    collector: TimeWindowPropertyCollector,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime:java.lang.Long  = null

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (null == windowEnd ||
        (null != windowEnd && currentRowTime > windowEnd)) {

        // calculate the current window and open a new window
        if (null != windowEnd) {
          // evaluate and emit the current window's result.
          doEvaluateAndCollect(
            aggregates,
            aggregateMapping,
            aggregateBuffer,
            output,
            out,
            collector,
            finalRowWindowStartPos,
            finalRowWindowEndPos,
            windowStart,
            windowEnd)
        } else {
          // set group keys value to final output.
          groupKeysMapping.foreach {
            case (after, previous) =>
              output.setField(after, record.getField(previous))
          }
        }
        // initiate intermediate aggregate value.
        aggregates.foreach(_.initiate(aggregateBuffer))
        windowStart = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      }

      // merge intermediate aggregate value to the buffered value.
      aggregates.foreach(_.merge(record, aggregateBuffer))

      windowEnd = if (isInputCombined) {
        // partial aggregate is supported
        record.getField(intermediateRowWindowEndPos).asInstanceOf[Long]
      } else {
        // partial aggregate is not supported, window-start equal rowtime + gap
        currentRowTime + gap
      }
    }
    // evaluate and emit the current window's result.
    doEvaluateAndCollect(
      aggregates,
      aggregateMapping,
      aggregateBuffer,
      output,
      out,
      collector,
      finalRowWindowStartPos,
      finalRowWindowEndPos,
      windowStart,
      windowEnd)
  }

  /**
    * Evaluate and emit the data of the current window.
    * @param windowStart the window's start attribute value is the min (rowtime)
    *                    of all rows in the window.
    * @param windowEnd the window's end property value is max (rowtime) + gap
    *                  for all rows in the window.
    */
  def doEvaluateAndCollect(
    aggregates: Array[Aggregate[_ <: Any]],
    aggregateMapping: Array[(Int, Int)],
    aggregateBuffer: Row,
    output: Row,
    out: Collector[Row],
    collector: TimeWindowPropertyCollector,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    windowStart: Long,
    windowEnd: Long): Unit = {

    // evaluate final aggregate value and set to output.
    aggregateMapping.foreach {
      case (after, previous) =>
        output.setField(after, aggregates(previous).evaluate(aggregateBuffer))
    }

    // adds TimeWindow properties to output then emit output
    if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
      collector.wrappedCollector = out
      collector.windowStart = windowStart
      collector.windowEnd = windowEnd

      collector.collect(output)
    } else {
      out.collect(output)
    }
  }
}
