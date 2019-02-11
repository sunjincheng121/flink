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

/**
  * The visitor definition of planner expression. It converts a PlannerExpression to an Expression.
  */
@Internal
trait PlannerExpressionVisitor[R] {

  def visit(expr: PlannerExpression): R

  def visitCall(call: PlannerCall): R

  def visitScalarFunctionCall(sfc: PlannerScalarFunctionCall): R

  def visitTableFunctionCall(tfc: PlannerTableFunctionCall): R

  def visitAggFunctionCall(afc: PlannerAggFunctionCall): R

  def visitDistinctAgg(distinctAgg: PlannerDistinctAgg): R

  def visitUnresolvedFieldReference(unresolvedFieldReference: PlannerUnresolvedFieldReference): R

  def visitResolvedFieldReference(resolvedFieldReference: PlannerResolvedFieldReference): R

  def visitTableReference(tableReference: PlannerTableReference): R

  def visitNull(plannerNull: PlannerNull): R

  def visitLiteral(literal: PlannerLiteral): R

  def visitTableSymbol(tableSymbol: SymbolPlannerExpression): R

}
