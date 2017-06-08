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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableEnvironment, TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions.ExpressionParser
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.sources._
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.sources.{StreamTableSource, TableSource}
import org.apache.flink.types.{LongValue, Record, Row}

/** Flink RelNode to read data from an external source defined by a [[StreamTableSource]]. */
class StreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSource: StreamTableSource[_])
  extends PhysicalTableSourceScan(cluster, traitSet, table, tableSource)
  with StreamScan {

  override def deriveRowType() = {
    val flinkTypeFactory = cluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]

    val fieldNames = TableEnvironment.getFieldNames(tableSource).toList
    val fieldTypes = TableEnvironment.getFieldTypes(tableSource.getReturnType).toList

    val fieldCnt = fieldNames.length

    val rowtime = tableSource match {
      case timeSource: DefinedRowtimeAttribute if timeSource.getRowtimeAttribute != null =>
        val rowtimeAttribute = timeSource.getRowtimeAttribute
        Some((fieldCnt, rowtimeAttribute))
      case _ =>
        None
    }

    val proctime = tableSource match {
      case timeSource: DefinedProctimeAttribute if timeSource.getProctimeAttribute != null =>
        val proctimeAttribute = timeSource.getProctimeAttribute
        Some((fieldCnt + (if (rowtime.isDefined) 1 else 0), proctimeAttribute))
      case _ =>
        None
    }

    flinkTypeFactory.buildLogicalRowType(
      fieldNames,
      fieldTypes,
      rowtime,
      proctime)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSource
    )
  }

  override def copy(
      traitSet: RelTraitSet,
      newTableSource: TableSource[_])
    : PhysicalTableSourceScan = {

    new StreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      newTableSource.asInstanceOf[StreamTableSource[_]]
    )
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.getConfig
    val inputDataStream = tableSource.getDataStream(tableEnv.execEnv).asInstanceOf[DataStream[Any]]

//    val streamType = inputDataStream.getType
//    val fieldNames = tableEnv.getFieldInfo(streamType)._1.map(ExpressionParser.parseExpression)
//
//    // validate and extract time attributes
//    val (rowtime, proctime, needDefaultTimestampAssigner): (Option[(Int, String)], Option[(Int,
//      String)], Boolean)=
//      tableEnv.validateAndExtractTimeAttributes(streamType, fieldNames)
//
//    // check if event-time is enabled
//    if (rowtime.isDefined && tableEnv.execEnv.getStreamTimeCharacteristic != TimeCharacteristic
//                                                                             .EventTime) {
//      throw TableException(
//        s"A rowtime attribute requires an EventTime time characteristic in stream environment. " +
//          s"But is: ${tableEnv.execEnv.getStreamTimeCharacteristic}")
//    }
//
//    if (rowtime.isDefined) {
//      if (inputDataStream.isInstanceOf[DataStreamSource[_]] && !needDefaultTimestampAssigner) {
//        throw new TableException(
//          "[.rowtime] on virtual column must call [assignTimestampsAndWatermarks] method.")
//      } else if (inputDataStream.isInstanceOf[SingleOutputStreamOperator[_]] &&
//        !inputDataStream.isInstanceOf[DataStreamSource[_]] && needDefaultTimestampAssigner) {
//        throw new TableException(
//          "[.rowtime] on already existing column must must not call " +
//            "[assignTimestampsAndWatermarks] method.")
//      }
//
//      if (needDefaultTimestampAssigner) {
//        val streamOperator = inputDataStream.assignTimestampsAndWatermarks(
//          new AssignerWithPunctuatedWatermarks[Any] {
//            override def checkAndGetNextWatermark(
//                lastElement: Any, extractedTimestamp: Long): Watermark = {
//              new Watermark(extractedTimestamp)
//            }
//
//
//            override def extractTimestamp(
//                element: Any,
//                previousElementTimestamp: Long): Long = {
//              element match {
//                case e: Product => e.productElement(rowtime.get._1).asInstanceOf[Long]
//                case e: Row => e.getField(rowtime.get._1).asInstanceOf[Long]
//                case e: Tuple => e.getField(rowtime.get._1).asInstanceOf[Long]
//                case e: Record => e.getField(rowtime.get._1, classOf[LongValue]).getValue
//                case e =>
//                  val typeInfo = TypeExtractor.createTypeInfo(element.getClass)
//                  if (typeInfo.isInstanceOf[PojoTypeInfo[_]]) {
//                    val getMethodName =
//                      "get".concat(rowtime.get._2.substring(0, 1).toUpperCase())
//                      .concat(rowtime.get._2.substring(1))
//                    element.getClass.getMethod(getMethodName).invoke(e).asInstanceOf[Long]
//                  } else {
//                    throw new TableException(
//                      s"Source of type ${typeInfo} cannot be converted into Table.")
//                  }
//              }
//            }
//          })
//        return convertToInternalRow(
//          new RowSchema(getRowType),
//          streamOperator,
//          new TableSourceTable(tableSource),
//          config)
//      }
//    }

    convertToInternalRow(
      new RowSchema(getRowType),
      inputDataStream,
      new TableSourceTable(tableSource),
      config)
  }
}
