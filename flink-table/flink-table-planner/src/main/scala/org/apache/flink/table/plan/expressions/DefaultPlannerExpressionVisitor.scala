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

package org.apache.flink.table.plan.expressions

import org.apache.flink.annotation.Internal
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.{FunctionDefinitions, _}

import _root_.scala.collection.JavaConversions._

/**
  * Default implementation of planner expression visitor.
  */
@Internal
class DefaultPlannerExpressionVisitor extends PlannerExpressionVisitor[Expression] {

  override def visit(expr: PlannerExpression): Expression = {
    expr match {
      case call: PlannerCall => visitCall(call)
      case sfc: PlannerScalarFunctionCall => visitScalarFunctionCall(sfc)
      case tfc: PlannerTableFunctionCall => visitTableFunctionCall(tfc)
      case afc: PlannerAggFunctionCall => visitAggFunctionCall(afc)
      case distinctAgg: PlannerDistinctAgg => visitDistinctAgg(distinctAgg)
      case ufr: PlannerUnresolvedFieldReference => visitUnresolvedFieldReference(ufr)
      case rfr: PlannerResolvedFieldReference => visitResolvedFieldReference(rfr)
      case tableReference: PlannerTableReference => visitTableReference(tableReference)
      case plannerNull: PlannerNull => visitNull(plannerNull)
      case literal: PlannerLiteral => visitLiteral(literal)
      case tableSymbol: SymbolPlannerExpression => visitTableSymbol(tableSymbol)
      case _ => throw new TableException(
        "PlannerExpression" + expr + " cannot be converted to an Expression.")
    }
  }

  override def visitCall(call: PlannerCall): Expression = {
    new Call(FunctionDefinitions.getDefinition(call.functionName), call.args.map(_.accept(this)))
  }

  override def visitScalarFunctionCall(sfc: PlannerScalarFunctionCall): Expression = {
    new Call(new ScalarFunctionDefinition(sfc.scalarFunction), sfc.parameters.map(_.accept(this)))
  }

  override def visitTableFunctionCall(tfc: PlannerTableFunctionCall): Expression = {
    val result = new TableFunctionCall(
      new TableFunctionDefinition(tfc.tableFunction, tfc.resultType),
      tfc.parameters.map(_.accept(this)))
    if (tfc.alias().isDefined) {
      result.alias(tfc.alias().get.toArray)
    }
    result
  }

  override def visitAggFunctionCall(afc: PlannerAggFunctionCall): Expression = {
    new Call(
      new AggregateFunctionDefinition(
        afc.aggregateFunction,
        afc.resultTypeInfo,
        afc.accTypeInfo),
      afc.args.map(_.accept(this)))
  }

  override def visitDistinctAgg(distinctAgg: PlannerDistinctAgg): Expression = {
    new DistinctAgg(distinctAgg.child.accept(this))
  }

  override def visitUnresolvedFieldReference(
      unresolvedFieldReference: PlannerUnresolvedFieldReference): Expression = {
    new FieldReference(unresolvedFieldReference.name)
  }

  override def visitResolvedFieldReference(
      resolvedFieldReference: PlannerResolvedFieldReference): Expression = {
    new FieldReference(resolvedFieldReference.name, resolvedFieldReference.resultType)
  }

  override def visitTableReference(tableReference: PlannerTableReference): Expression = {
    new TableReference(tableReference.name, tableReference.table)
  }

  override def visitNull(plannerNull: PlannerNull): Expression = {
    new Literal(null, plannerNull.resultType)
  }

  override def visitLiteral(literal: PlannerLiteral): Expression = {
    if (literal.resultType == null) {
      new Literal(literal.value)
    } else {
      new Literal(literal.value, literal.resultType)
    }
  }

  override def visitTableSymbol(tableSymbol: SymbolPlannerExpression): Expression = {
    val symbol = tableSymbol.symbol match {
      case PlannerTimeIntervalUnit.YEAR => TimeIntervalUnit.YEAR
      case PlannerTimeIntervalUnit.YEAR_TO_MONTH => TimeIntervalUnit.YEAR_TO_MONTH
      case PlannerTimeIntervalUnit.QUARTER => TimeIntervalUnit.QUARTER
      case PlannerTimeIntervalUnit.MONTH => TimeIntervalUnit.MONTH
      case PlannerTimeIntervalUnit.WEEK => TimeIntervalUnit.WEEK
      case PlannerTimeIntervalUnit.DAY => TimeIntervalUnit.DAY
      case PlannerTimeIntervalUnit.DAY_TO_HOUR => TimeIntervalUnit.DAY_TO_HOUR
      case PlannerTimeIntervalUnit.DAY_TO_MINUTE => TimeIntervalUnit.DAY_TO_MINUTE
      case PlannerTimeIntervalUnit.DAY_TO_SECOND => TimeIntervalUnit.DAY_TO_SECOND
      case PlannerTimeIntervalUnit.HOUR => TimeIntervalUnit.HOUR
      case PlannerTimeIntervalUnit.HOUR_TO_MINUTE => TimeIntervalUnit.HOUR_TO_MINUTE
      case PlannerTimeIntervalUnit.HOUR_TO_SECOND => TimeIntervalUnit.HOUR_TO_SECOND
      case PlannerTimeIntervalUnit.MINUTE => TimeIntervalUnit.MINUTE
      case PlannerTimeIntervalUnit.MINUTE_TO_SECOND => TimeIntervalUnit.MINUTE_TO_SECOND
      case PlannerTimeIntervalUnit.SECOND => TimeIntervalUnit.SECOND

      case PlannerTimePointUnit.YEAR => TimePointUnit.YEAR
      case PlannerTimePointUnit.MONTH => TimePointUnit.MONTH
      case PlannerTimePointUnit.DAY => TimePointUnit.DAY
      case PlannerTimePointUnit.HOUR => TimePointUnit.HOUR
      case PlannerTimePointUnit.MINUTE => TimePointUnit.MINUTE
      case PlannerTimePointUnit.SECOND => TimePointUnit.SECOND
      case PlannerTimePointUnit.QUARTER => TimePointUnit.QUARTER
      case PlannerTimePointUnit.WEEK => TimePointUnit.WEEK
      case PlannerTimePointUnit.MILLISECOND => TimePointUnit.MILLISECOND
      case PlannerTimePointUnit.MICROSECOND => TimePointUnit.MICROSECOND

      case PlannerTrimMode.BOTH => TrimMode.BOTH
      case PlannerTrimMode.LEADING => TrimMode.LEADING
      case PlannerTrimMode.TRAILING => TrimMode.TRAILING

      case _ =>
        throw new TableException("unsupported TableSymbolValue: " + tableSymbol.symbol.symbols)
    }
    new SymbolExpression(symbol)
  }
}
