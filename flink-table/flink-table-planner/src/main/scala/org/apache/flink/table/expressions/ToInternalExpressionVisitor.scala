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

class ToInternalExpressionVisitor extends ExpressionVisitor[PlannerExpression] {

  override def visitCall(call: Call): PlannerExpression = {

    val func = call.getFunc
    val args = call.getChildren.asScala

    func match {
      case e: ScalarFunctionDefinition =>
        PlannerScalarFunctionCall(e.getFunc, args.map(_.accept(this)))

      case aggFuncDef: AggFunctionDefinition =>
        PlannerAggFunctionCall(
          aggFuncDef.getFunc,
          aggFuncDef.getResultTypeInfo,
          aggFuncDef.getAccTypeInfo,
          args.map(_.accept(this)))

      case e: FunctionDefinition =>
        e match {
          case FunctionDefinitions.CAST =>
            assert(args.size == 2)
            PlannerCast(args.head.accept(this), args.last.asInstanceOf[TypeLiteral].getType)

          case FunctionDefinitions.FLATTENING =>
            assert(args.size == 1)
            PlannerFlattening(args.head.accept(this))

          case FunctionDefinitions.GET_COMPOSITE_FIELD =>
            assert(args.size == 2)
            PlannerGetCompositeField(args.head.accept(this),
              args.last.asInstanceOf[Literal].getValue)

          case FunctionDefinitions.IN =>
            PlannerIn(args.head.accept(this), args.slice(1, args.size).map(_.accept(this)))

          case _ => PlannerCall(e.name, args.map(_.accept(this)))
        }
    }
  }

  override def visitUnresolvedOverCall(overCall: UnresolvedOverCall): PlannerExpression = {
    PlannerUnresolvedOverCall(overCall.getLeft.accept(this), overCall.getRight.accept(this))
  }

  override def visitDistinctAgg(distinctAgg: DistinctAgg): PlannerExpression = {
    PlannerDistinctAgg(distinctAgg.getChild.accept(this))
  }

  override def visitAlias(alias: Alias): PlannerExpression = {
    PlannerAlias(alias.getChild.accept(this), alias.getName, alias.getExtraNames.asScala)
  }

  override def visitRowtimeAttribute(rowtimeAttribute: RowtimeAttribute): PlannerExpression = {
    PlannerRowtimeAttribute(rowtimeAttribute.getChild.accept(this))
  }

  override def visitProctimeAttribute(proctimeAttribute: ProctimeAttribute): PlannerExpression = {
    PlannerProctimeAttribute(proctimeAttribute.getChild.accept(this))
  }

  // needn't process TypeLiteral directly. It is processed with Cast
  override def visitTypeLiteral(typeLiteral: TypeLiteral): PlannerExpression = ???

  override def visitUnboundedRange(unboundedRange: UnboundedRange): PlannerExpression = {
    PlannerUnboundedRange()
  }

  override def visitCurrentRange(currentRange: CurrentRange): PlannerExpression = {
    PlannerCurrentRange()
  }

  override def visitNull(_null: Null): PlannerExpression = {
    PlannerNull(_null.getType)
  }

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
    PlannerSymbolExpression(tableSymbol)
  }

  override def visitLiteral(literal: Literal): PlannerExpression = {
    if (!literal.getType.isPresent) {
      PlannerLiteral(literal.getValue)
    } else {
      PlannerLiteral(literal.getValue, literal.getType.get())
    }
  }

  override def visitCurrentRow(currentRow: CurrentRow): PlannerExpression = {
    PlannerCurrentRow()
  }

  override def visitTableReference(tableReference: TableReference): PlannerExpression = {
    PlannerTableReference(tableReference.getName, tableReference.getTable)
  }

  override def visitUnboundedRow(unboundedRow: UnboundedRow): PlannerExpression = {
    PlannerUnboundedRow()
  }

  override def visitUnresolvedFieldReference(unresolvedFieldReference: UnresolvedFieldReference)
  : PlannerExpression = {
    PlannerUnresolvedFieldReference(unresolvedFieldReference.getName)
  }
}
