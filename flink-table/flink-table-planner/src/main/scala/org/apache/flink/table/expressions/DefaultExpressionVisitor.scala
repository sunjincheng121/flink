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

package org.apache.flink.table.expressions

import org.apache.flink.table.api._
import org.apache.flink.table.plan.expressions._

import _root_.scala.collection.JavaConverters._

/**
  * Default implementation of expression visitor.
  */
class DefaultExpressionVisitor extends ExpressionVisitor[PlannerExpression] {

  override def visitCall(call: Call): PlannerExpression = {

    val func = call.getFunc
    val args = call.getChildren.asScala

    func match {
      case e: ScalarFunctionDefinition =>
        PlannerScalarFunctionCall(e.getScalarFunction, args.map(_.accept(this)))

      case e: TableFunctionDefinition =>
        val tfc = PlannerTableFunctionCall(
          e.getName, e.getTableFunction, args.map(_.accept(this)), e.getResultType)
        val alias = call.asInstanceOf[TableFunctionCall].alias()
        if (alias.isPresent) {
          tfc.setAliases(alias.get())
        }
        tfc

      case aggFuncDef: AggregateFunctionDefinition =>
        PlannerAggFunctionCall(
          aggFuncDef.getAggregateFunction,
          aggFuncDef.getResultTypeInfo,
          aggFuncDef.getAccumulatorTypeInfo,
          args.map(_.accept(this)))

      case e: FunctionDefinition =>
        e match {
          case FunctionDefinitions.CAST =>
            assert(args.size == 2)
            PlannerCast(args.head.accept(this), args.last.asInstanceOf[TypeLiteral].getType)

          case FunctionDefinitions.AS =>
            assert(args.size >= 2)
            val child = args(0)
            val name = args(1).asInstanceOf[Literal].getValue.asInstanceOf[String]
            val extraNames =
              args.drop(1).drop(1).map(e => e.asInstanceOf[Literal].getValue.asInstanceOf[String])
            PlannerAlias(child.accept(this), name, extraNames)

          case FunctionDefinitions.FLATTEN =>
            assert(args.size == 1)
            PlannerFlattening(args.head.accept(this))

          case FunctionDefinitions.GET_COMPOSITE_FIELD =>
            assert(args.size == 2)
            PlannerGetCompositeField(args.head.accept(this),
              args.last.asInstanceOf[Literal].getValue)

          case FunctionDefinitions.IN =>
            PlannerIn(args.head.accept(this), args.slice(1, args.size).map(_.accept(this)))

          case FunctionDefinitions.PROC_TIME =>
            PlannerProctimeAttribute(args.head.accept(this))

          case FunctionDefinitions.ROW_TIME =>
            PlannerRowtimeAttribute(args.head.accept(this))

          case FunctionDefinitions.UNBOUNDED_RANGE =>
            PlannerUnboundedRange()

          case FunctionDefinitions.OVER_CALL =>
            PlannerUnresolvedOverCall(
              args(0).accept(this),
              args(1).accept(this)
            )

          case _ => PlannerCall(e.getName, args.map(_.accept(this)))
        }
    }
  }

  override def visitDistinctAgg(distinctAgg: DistinctAgg): PlannerExpression = {
    PlannerDistinctAgg(distinctAgg.getChildren.get(0).accept(this))
  }

  // needn't process TypeLiteral directly. It is processed with Cast
  override def visitTypeLiteral(typeLiteral: TypeLiteral): PlannerExpression = ???

  override def visitSymbolExpression(symbolExpression: SymbolExpression): PlannerExpression = {
    val tableSymbol = symbolExpression.getSymbol match {
      case TimeIntervalUnit.YEAR => PlannerTimeIntervalUnit.YEAR
      case TimeIntervalUnit.YEAR_TO_MONTH => PlannerTimeIntervalUnit.YEAR_TO_MONTH
      case TimeIntervalUnit.QUARTER => PlannerTimeIntervalUnit.QUARTER
      case TimeIntervalUnit.MONTH => PlannerTimeIntervalUnit.MONTH
      case TimeIntervalUnit.WEEK => PlannerTimeIntervalUnit.WEEK
      case TimeIntervalUnit.DAY => PlannerTimeIntervalUnit.DAY
      case TimeIntervalUnit.DAY_TO_HOUR => PlannerTimeIntervalUnit.DAY_TO_HOUR
      case TimeIntervalUnit.DAY_TO_MINUTE => PlannerTimeIntervalUnit.DAY_TO_MINUTE
      case TimeIntervalUnit.DAY_TO_SECOND => PlannerTimeIntervalUnit.DAY_TO_SECOND
      case TimeIntervalUnit.HOUR => PlannerTimeIntervalUnit.HOUR
      case TimeIntervalUnit.HOUR_TO_MINUTE => PlannerTimeIntervalUnit.HOUR_TO_MINUTE
      case TimeIntervalUnit.HOUR_TO_SECOND => PlannerTimeIntervalUnit.HOUR_TO_SECOND
      case TimeIntervalUnit.MINUTE => PlannerTimeIntervalUnit.MINUTE
      case TimeIntervalUnit.MINUTE_TO_SECOND => PlannerTimeIntervalUnit.MINUTE_TO_SECOND
      case TimeIntervalUnit.SECOND => PlannerTimeIntervalUnit.SECOND

      case TimePointUnit.YEAR => PlannerTimePointUnit.YEAR
      case TimePointUnit.MONTH => PlannerTimePointUnit.MONTH
      case TimePointUnit.DAY => PlannerTimePointUnit.DAY
      case TimePointUnit.HOUR => PlannerTimePointUnit.HOUR
      case TimePointUnit.MINUTE => PlannerTimePointUnit.MINUTE
      case TimePointUnit.SECOND => PlannerTimePointUnit.SECOND
      case TimePointUnit.QUARTER => PlannerTimePointUnit.QUARTER
      case TimePointUnit.WEEK => PlannerTimePointUnit.WEEK
      case TimePointUnit.MILLISECOND => PlannerTimePointUnit.MILLISECOND
      case TimePointUnit.MICROSECOND => PlannerTimePointUnit.MICROSECOND

      case TrimMode.BOTH => PlannerTrimMode.BOTH
      case TrimMode.LEADING => PlannerTrimMode.LEADING
      case TrimMode.TRAILING => PlannerTrimMode.TRAILING

      case _ =>
        throw new TableException("unsupported TableSymbolValue: " + symbolExpression.getSymbol)
    }
    SymbolPlannerExpression(tableSymbol)
  }

  override def visitLiteral(literal: Literal): PlannerExpression = {
    if (!literal.getType.isPresent) {
      PlannerLiteral(literal.getValue)
    } else if (literal.getValue == null) {
      PlannerNull(literal.getType.get())
    } else {
      PlannerLiteral(literal.getValue, literal.getType.get())
    }
  }

  override def visitOther(other: Expression): PlannerExpression = {
    other match {
      case e: TableReference => PlannerTableReference(
        e.asInstanceOf[TableReference].getName,
        e.asInstanceOf[TableReference].getTable)
      case _ =>
        throw new TableException("Unrecognized expression [" + other + "]")
    }

  }

  override def visitFieldReference(unresolvedFieldReference: FieldReference)
  : PlannerExpression = {
    PlannerUnresolvedFieldReference(unresolvedFieldReference.getName)
  }
}
