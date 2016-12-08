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
package org.apache.flink.api.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.functions.{FlatJoinFunction, RichFlatJoinFunction}
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.codegen.CodeGenerator
import org.apache.flink.api.table.runtime.FlatJoinRunner
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.{StreamTableEnvironment, TableException}
import org.apache.flink.api.table.typeutils.TypeConverter._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.table.functions.{StreamConnectCoMapFunction}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.expressions.ResolvedFieldReference

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by jincheng.sunjc on 16/12/7.
  */
class DataStreamJoin (
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  leftNode: RelNode,
  rightNode: RelNode,
  rowRelDataType: RelDataType,
  joinCondition: RexNode,
  joinRowType: RelDataType,
  joinInfo: JoinInfo,
  keyPairs: List[IntPair],
  joinType: JoinRelType,
  joinHint: JoinHint,
  ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
          with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
    .item("where", joinConditionToString)
    .item("join", joinSelectionToString)
    .item("joinType", joinTypeToString)
  }
  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val children = this.getInputs
    children.foldLeft(planner.getCostFactory.makeCost(0, 0, 0)) { (cost, child) =>
      val rowCnt = metadata.getRowCount(child)
      val rowSize = 2
      cost.plus(planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize))
    }
  }
  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv     The [[StreamTableEnvironment]] of the translated Table.
    * @param expectedType specifies the type the Flink operator should return. The type must
    *                     have the same arity as the result. For instance, if the
    *                     expected type is a RowTypeInfo this method will return a DataSet of
    *                     type Row. If the expected type is Tuple2, the operator will return
    *                     a Tuple2 if possible. Row otherwise.
    * @return DataStream of type expectedType or RowTypeInfo
    */
  override def translateToPlan(
    tableEnv: StreamTableEnvironment,
    expectedType: Option[TypeInformation[Any]]): DataStream[Any] = {
    val config = tableEnv.getConfig

    val returnType = determineReturnType(
      getRowType,
      expectedType,
      config.getNullCheck,
      config.getEfficientTypeUsage)

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]

    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
      )
    }
    else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: ($joinConditionToString)"
          )
        }
      })
    }

    val leftDataStream = left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val rightDataStream = right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val (connectOperator, nullCheck) = joinType match {
      case JoinRelType.INNER => (leftDataStream.connect(rightDataStream), false)
      case _ => throw new UnsupportedOperationException(s"Un supported JoinType [ $joinType ]")
    }

    if (nullCheck && !config.getNullCheck) {
      throw TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val generator = new CodeGenerator(
      config,
      nullCheck,
      leftDataStream.getType,
      Some(rightDataStream.getType))
    val conversion = generator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    var body = ""

    if (joinInfo.isEqui) {
      // only equality condition
      body = s"""
                |${conversion.code}
                |${generator.collectorTerm}.collect(${conversion.resultTerm});
                |""".stripMargin
    }
    else {
      val condition = generator.generateExpression(joinCondition)
      body = s"""
                |${condition.code}
                |if (${condition.resultTerm}) {
                |  ${conversion.code}
                |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
                |}
                |""".stripMargin
    }
    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Any, Any, Any]],
      body,
      returnType)

    val joinFun = new FlatJoinRunner[Any, Any, Any](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)
    val joinOpName = s"where: ($joinConditionToString), join: ($joinSelectionToString)"

    val coMapFun =
      new StreamConnectCoMapFunction[Any,Any, Any](
        joinFun,
        leftKeys.toArray,
        rightKeys.toArray,
        returnType.asInstanceOf[CompositeType[Any]])

    connectOperator
    .keyBy(leftKeys.toArray, rightKeys.toArray)
    .flatMap[Any](coMapFun).name(joinOpName).asInstanceOf[DataStream[Any]]
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {

    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT=> "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }
}
