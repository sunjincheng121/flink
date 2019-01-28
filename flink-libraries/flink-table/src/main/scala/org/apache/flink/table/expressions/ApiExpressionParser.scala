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
import org.apache.flink.table.apiexpressions._

object ApiExpressionParser {
  def parse(expr: ApiExpression): Expression = {
    if (expr == null) {
      return null
    }
    expr match {
      case ApiDistinctAgg(child) =>
        DistinctAgg(parse(child))

      case ApiAggFunctionCall(function, resultTypeInfo, accTypeInfo, args) =>
        AggFunctionCall(function, resultTypeInfo, accTypeInfo, args.map(parse))

      case ApiCall(functionName, args) =>
        Call(functionName, args.map(parse))

      case ApiUnresolvedOverCall(agg, alias) =>
        UnresolvedOverCall(parse(agg), parse(alias))

      case ApiScalarFunctionCall(scalarFunction, parameters) =>
        ScalarFunctionCall(scalarFunction, parameters.map(parse))

      case ApiTableFunctionCall(functionName, tableFunction, parameters, resultType) =>
        TableFunctionCall(functionName, tableFunction, parameters.map(parse), resultType)

      case ApiCast(child, resultType) =>
        Cast(parse(child), resultType)

      case ApiFlattening(child) =>
        Flattening(parse(child))

      case ApiGetCompositeField(child, key) =>
        GetCompositeField(parse(child), key)

      case ApiUnresolvedFieldReference(name) =>
        UnresolvedFieldReference(name)

      case ApiAlias(child, name, extraNames) =>
        Alias(parse(child), name, extraNames)

      case ApiTableReference(name, table) =>
        TableReference(name, table)

      case ApiRowtimeAttribute(expression) =>
        RowtimeAttribute(parse(expression))

      case ApiProctimeAttribute(expression) =>
        ProctimeAttribute(parse(expression))

      case ApiStreamRecordTimestamp() =>
        StreamRecordTimestamp()

      case ApiLiteral(l, None) =>
        Literal(l)

      case ApiLiteral(l, Some(t)) =>
        Literal(l, t)

      case ApiNull(resultType) =>
        Null(resultType)

      case ApiIn(expression, elements) =>
        In(parse(expression), elements.map(parse))

      case ApiCurrentRow() =>
        CurrentRow()

      case ApiCurrentRange() =>
        CurrentRange()

      case ApiUnboundedRow() =>
        UnboundedRow()

      case ApiUnboundedRange() =>
        UnboundedRange()

      case ApiSymbolExpression(symbol) =>
        val tableSymbol = symbol match {
          case ApiTimeIntervalUnit.YEAR => TimeIntervalUnit.YEAR
          case ApiTimeIntervalUnit.YEAR_TO_MONTH => TimeIntervalUnit.YEAR_TO_MONTH
          case ApiTimeIntervalUnit.QUARTER => TimeIntervalUnit.QUARTER
          case ApiTimeIntervalUnit.MONTH => TimeIntervalUnit.MONTH
          case ApiTimeIntervalUnit.WEEK => TimeIntervalUnit.WEEK
          case ApiTimeIntervalUnit.DAY => TimeIntervalUnit.DAY
          case ApiTimeIntervalUnit.DAY_TO_HOUR => TimeIntervalUnit.DAY_TO_HOUR
          case ApiTimeIntervalUnit.DAY_TO_MINUTE => TimeIntervalUnit.DAY_TO_MINUTE
          case ApiTimeIntervalUnit.DAY_TO_SECOND => TimeIntervalUnit.DAY_TO_SECOND
          case ApiTimeIntervalUnit.HOUR => TimeIntervalUnit.HOUR
          case ApiTimeIntervalUnit.HOUR_TO_MINUTE => TimeIntervalUnit.HOUR_TO_MINUTE
          case ApiTimeIntervalUnit.HOUR_TO_SECOND => TimeIntervalUnit.HOUR_TO_SECOND
          case ApiTimeIntervalUnit.MINUTE => TimeIntervalUnit.MINUTE
          case ApiTimeIntervalUnit.MINUTE_TO_SECOND => TimeIntervalUnit.MINUTE_TO_SECOND
          case ApiTimeIntervalUnit.SECOND => TimeIntervalUnit.SECOND

          case ApiTimePointUnit.YEAR => TimePointUnit.YEAR
          case ApiTimePointUnit.MONTH => TimePointUnit.MONTH
          case ApiTimePointUnit.DAY => TimePointUnit.DAY
          case ApiTimePointUnit.HOUR => TimePointUnit.HOUR
          case ApiTimePointUnit.MINUTE => TimePointUnit.MINUTE
          case ApiTimePointUnit.SECOND => TimePointUnit.SECOND
          case ApiTimePointUnit.QUARTER => TimePointUnit.QUARTER
          case ApiTimePointUnit.WEEK => TimePointUnit.WEEK
          case ApiTimePointUnit.MILLISECOND => TimePointUnit.MILLISECOND
          case ApiTimePointUnit.MICROSECOND => TimePointUnit.MICROSECOND

          case ApiTrimMode.BOTH => TrimMode.BOTH
          case ApiTrimMode.LEADING => TrimMode.LEADING
          case ApiTrimMode.TRAILING => TrimMode.TRAILING

          case _ =>
            throw new TableException("unsupported TableSymbolValue: " + symbol)
        }
        SymbolExpression(tableSymbol)

      case _ =>
        throw new TableException("unsupported Expression: " + expr.getClass.getSimpleName)
    }
  }
}
