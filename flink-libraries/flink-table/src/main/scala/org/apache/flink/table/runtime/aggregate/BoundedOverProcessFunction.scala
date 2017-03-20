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

import java.lang.{Long => JLong}
import java.util
import java.util.{ArrayList, List => JList}

import org.apache.flink.api.common.state._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.apache.flink.table.TableAPIConfigConstant._

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

/**
  * Process Function for bounded Over window
  *
  * @param aggregates the list of all [[org.apache.flink.table.functions.AggregateFunction]]
  *                   used for this aggregation
  * @param aggFields the position (in the input Row) of the input value for each aggregate
  * @param forwardedFieldCount the count of forwarded fields.
  * @param aggregationStateType the row type info of aggregation
  * @param inputType the row type info for input data
  * @param processingOffset the offset of processing
  * @param isEvenTime It is a tag that indicates whether the time type is Event-Time
  */
class BoundedOverProcessFunction(
    private val aggregates: Array[AggregateFunction[_]],
    private val aggFields: Array[Int],
    private val forwardedFieldCount: Int,
    private val aggregationStateType: RowTypeInfo,
    private val inputType: RowTypeInfo,
    private val processingOffset: Int,
    private val isEvenTime: Boolean)
  extends ProcessFunction[Row, Row] with CheckpointedFunction {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var sameTimeDataBuffer: JList[Row] = _

  // save last expired data
  private var lastExpiredRow: Row = _
  // persistent last expired data for recovery
  private var lastExpiredRowState: ListState[Row] = null

  // for event-time, sorted the data by timestamp
  private var bufferedRowPriorityQueue: PriorityQueue[(Long, Long)] = _
  // for proc-time
  private var bufferedRowList: JList[Long] = _
  // persistent buffered data for recovery
  private var bufferOperatorState: ListState[Long] = null


  // save incremental calculation accumulator
  private var accumulatorState: ValueState[Row] = _

  // save offset window data records
  private var windowDataState: MapState[Long, JList[Row]] = _

  // save the time at which the current node triggers the calculation
  private var lastTriggerTs: JLong = JLong.MIN_VALUE

  // the length of time that the user configures the allowable data delay
  private var allowedLateness = 0L

  override def open(config: Configuration) {

    val jobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    if (null != jobParameters) {
      val allowedLatenessStr = jobParameters.toMap.get(OVER_EVENT_TIME_ALLOWED_LATENESS)
      if (null != allowedLatenessStr)
        allowedLateness = allowedLatenessStr.toLong
    }

    output = new Row(forwardedFieldCount + aggregates.length)
    sameTimeDataBuffer = new ArrayList[Row]()

    val stateDescriptor: ValueStateDescriptor[Row] =
      new ValueStateDescriptor[Row]("overState", aggregationStateType)
    accumulatorState = getRuntimeContext.getState(stateDescriptor)

    val mapStateDescriptor: MapStateDescriptor[Long, JList[Row]] =
      new MapStateDescriptor[Long, JList[Row]](
        "overMapState",
        classOf[Long],
        classOf[JList[Row]])

    windowDataState = getRuntimeContext.getMapState(mapStateDescriptor)

    if (isEvenTime) {
      implicit val ord: Ordering[(Long, Long)] = Ordering.by(_._2)
      bufferedRowPriorityQueue = new mutable.PriorityQueue[(Long, Long)]()
    } else {
      bufferedRowList = new ArrayList[Long]()
    }
  }

  override def processElement(
    input: Row,
    ctx: ProcessFunction[Row, Row]#Context,
    out: Collector[Row]): Unit = {

    if (isEvenTime) {
      // trigger timestamp for trigger calculation
      val triggerTimestamp = ctx.timestamp + allowedLateness
      // check if the data is expired, if not, save the data and register event time timer
      if (triggerTimestamp > lastTriggerTs
          && triggerTimestamp > ctx.timerService.currentWatermark()) {
        if (windowDataState.contains(triggerTimestamp)) {
          val data = windowDataState.get(triggerTimestamp)
          data.add(input)
          windowDataState.put(triggerTimestamp, data)
        } else {
          val data = new ArrayList[Row]()
          data.add(input)
          windowDataState.put(triggerTimestamp, data)
          // register event time timer
          ctx.timerService().registerEventTimeTimer(triggerTimestamp)
        }
      }
    } else {
      // trigger timestamp for trigger calculation
      val triggerTimestamp = ctx.timerService().currentProcessingTime()

      if (windowDataState.contains(triggerTimestamp)) {
        val data = windowDataState.get(triggerTimestamp)
        data.add(input)
        windowDataState.put(triggerTimestamp, data)
      } else {
        val data = new ArrayList[Row]()
        data.add(input)
        windowDataState.put(triggerTimestamp, data)
      }
      doProc(triggerTimestamp, input, out)
    }

  }

  override def onTimer(
    timestamp: Long,
    ctx: ProcessFunction[Row, Row]#OnTimerContext,
    out: Collector[Row]): Unit = {

    lastTriggerTs = timestamp
    var accumulators = accumulatorState.value()

    // first run or recovery
    if (null == accumulators) {
      lastExpiredRow = null
      if (isEvenTime) {
        bufferedRowPriorityQueue.clear()
      } else {
        bufferedRowList.clear()
      }
      accumulators = new Row(aggregates.length)
      var i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    // initialize the data from the state
      if (bufferedRowPriorityQueue.isEmpty) {
        val bufferPriorityIt = bufferOperatorState.get().iterator()
        while (bufferPriorityIt.hasNext) {
          val ts = bufferPriorityIt.next()
          bufferedRowPriorityQueue.enqueue((ts, Long.MaxValue - ts))
        }
      }

    // gets the window data for the calculation from state
    val inputs: JList[Row] = windowDataState.get(timestamp)
    if (null != inputs) {
      var j: Int = 0
      while (j < inputs.size) {
        val input = inputs.get(j)

        if (null == lastExpiredRow) {
          val lastValueOperatorIt = lastExpiredRowState.get().iterator()
          if (lastValueOperatorIt.hasNext) {
            lastExpiredRow = lastValueOperatorIt.next()
          }
        }

          // save current timestamp to buffered row priority-queue which manage window data
          bufferedRowPriorityQueue.enqueue((timestamp, Long.MaxValue - timestamp))
          if (bufferedRowPriorityQueue.size > processingOffset) {
            val expiredRowTimestamp = bufferedRowPriorityQueue.dequeue()
            val windowDataList = windowDataState.get(expiredRowTimestamp._1)

            lastExpiredRow =windowDataList.get(0)
            windowDataList.remove(0)

            if (windowDataList.size() > 0) {
              windowDataState.put(expiredRowTimestamp._1, windowDataList)
            } else {
              windowDataState.remove(expiredRowTimestamp._1)
            }
          }

        var i = 0
        while (i < forwardedFieldCount) {
          output.setField(i, input.getField(i))
          i += 1
        }

        if (null != lastExpiredRow) {
          i = 0
          while (i < aggregates.length) {
            val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
            val agg = aggregates(i)
            aggregates(i).retract(accumulator, lastExpiredRow.getField(aggFields(i)))
            i += 1
          }
        }

        i = 0
        while (i < aggregates.length) {
          val index = forwardedFieldCount + i
          val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
          aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
          output.setField(index, aggregates(i).getValue(accumulator))
          i += 1
        }
        accumulatorState.update(accumulators)

        out.collect(output)
        j = j + 1
      }
    }

  }

  def doProc(
    timestamp: Long,
      input: Row,
    out: Collector[Row]): Unit = {

    var accumulators = accumulatorState.value()

    // first run or recovery
    if (null == accumulators) {
      lastExpiredRow = null
      if (isEvenTime) {
        bufferedRowPriorityQueue.clear()
      } else {
        bufferedRowList.clear()
      }
      accumulators = new Row(aggregates.length)
      var i = 0
      while (i < aggregates.length) {
        accumulators.setField(i, aggregates(i).createAccumulator())
        i += 1
      }
    }

    // initialize the data from the state
      if (bufferedRowList.isEmpty) {
        val bufferPriorityIt = bufferOperatorState.get().iterator()
        while (bufferPriorityIt.hasNext) {
          val ts = bufferPriorityIt.next()
          bufferedRowList.add(ts)
        }
      }

        if (null == lastExpiredRow) {
          val lastValueOperatorIt = lastExpiredRowState.get().iterator()
          if (lastValueOperatorIt.hasNext) {
            lastExpiredRow = lastValueOperatorIt.next()
          }
        }

          // save current timestamp to buffered row list which manage window data
          bufferedRowList.add(timestamp)
          if (bufferedRowList.size >= processingOffset) {
            val expiredRowTimestamp = bufferedRowList.get(0)
            bufferedRowList.remove(0)
            val windowDataList = windowDataState.get(expiredRowTimestamp)

            lastExpiredRow = windowDataList.get(0)

            windowDataList.remove(0)

            if (windowDataList.size() > 0) {
              windowDataState.put(expiredRowTimestamp, windowDataList)
            } else {
              windowDataState.remove(expiredRowTimestamp)
            }
          }

        var i = 0
        while (i < forwardedFieldCount) {
          output.setField(i, input.getField(i))
          i += 1
        }

        if (null != lastExpiredRow) {
          i = 0
          while (i < aggregates.length) {
            val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
            val agg = aggregates(i)
            aggregates(i).retract(accumulator, lastExpiredRow.getField(aggFields(i)))
            i += 1
          }
        }

        i = 0
        while (i < aggregates.length) {
          val index = forwardedFieldCount + i
          val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
          aggregates(i).accumulate(accumulator, input.getField(aggFields(i)))
          output.setField(index, aggregates(i).getValue(accumulator))
          i += 1
        }
        accumulatorState.update(accumulators)

        out.collect(output)
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    accumulatorState.clear()
    lastExpiredRowState.add(lastExpiredRow)
    if (isEvenTime) {
      while (bufferedRowPriorityQueue.nonEmpty) {
        bufferOperatorState.add(bufferedRowPriorityQueue.dequeue()._1)
      }
    } else {
      var i = 0
      while (i < bufferedRowList.size()) {
        bufferOperatorState.add(bufferedRowList.get(i))
        i = i + 1
      }
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val LastValueOperatorStateSerializer = inputType.createSerializer(getRuntimeContext.getExecutionConfig)
    val LastValueOperatorDescriptor =
      new ListStateDescriptor[Row]("lastValueOperatorState", LastValueOperatorStateSerializer)
    lastExpiredRowState =
        context.getOperatorStateStore.getOperatorState(LastValueOperatorDescriptor)
    val bufferPriorityQueueOperatorDescriptor =
      new ListStateDescriptor[Long]("bufferPriorityQueueOOperatorState", classOf[Long])
    bufferOperatorState =
        context.getOperatorStateStore.getOperatorState(bufferPriorityQueueOperatorDescriptor)
  }

}
