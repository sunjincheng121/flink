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

package org.apache.flink.table.apiexpressions

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.{Table, ValidationException}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.TreeNode
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

abstract class ApiExpression extends TreeNode[ApiExpression]

abstract class ApiBinaryExpression extends ApiExpression {
  private[flink] def left: ApiExpression
  private[flink] def right: ApiExpression
  private[flink] def children = Seq(left, right)
}

abstract class ApiUnaryExpression extends ApiExpression {
  private[flink] def child: ApiExpression
  private[flink] def children = Seq(child)
}

abstract class ApiLeafExpression extends ApiExpression {
  private[flink] val children = Nil
}

case class ApiDistinctAgg(child: ApiExpression) extends ApiUnaryExpression

case class ApiAggFunctionCall(aggregateFunction: AggregateFunction[_, _],
                              resultTypeInfo: TypeInformation[_],
                              accTypeInfo: TypeInformation[_],
                              args: Seq[ApiExpression])
  extends ApiExpression {
  override def children: Seq[ApiExpression] = args
}

case class ApiCall(functionName: String, args: Seq[ApiExpression]) extends ApiExpression {
  override def children: Seq[ApiExpression] = args
}

case class ApiUnresolvedOverCall(agg: ApiExpression, alias: ApiExpression)
  extends ApiExpression {
  override private[flink] def children: Seq[ApiExpression] = Seq(agg, alias)
}

case class ApiScalarFunctionCall(
    scalarFunction: ScalarFunction,
    parameters: Seq[ApiExpression])
  extends ApiExpression {
  override private[flink] def children: Seq[ApiExpression] = parameters
}

case class ApiTableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[ApiExpression],
    resultType: TypeInformation[_])
  extends ApiExpression {
  override private[flink] def children: Seq[ApiExpression] = parameters
}

case class ApiCast(child: ApiExpression, resultType: TypeInformation[_]) extends ApiUnaryExpression

case class ApiFlattening(child: ApiExpression) extends ApiUnaryExpression

case class ApiGetCompositeField(child: ApiExpression, key: Any) extends ApiUnaryExpression

case class ApiUnresolvedFieldReference(name: String) extends ApiLeafExpression

case class ApiAlias(child: ApiExpression, name: String, extraNames: Seq[String] = Seq())
  extends ApiUnaryExpression

case class ApiTableReference(name: String, table: Table) extends ApiLeafExpression

case class ApiRowtimeAttribute(expr: ApiExpression) extends ApiUnaryExpression {
  override private[flink] def child: ApiExpression = expr
}

case class ApiProctimeAttribute(expr: ApiExpression) extends ApiUnaryExpression {
  override private[flink] def child: ApiExpression = expr
}

case class ApiStreamRecordTimestamp() extends ApiLeafExpression

case class ApiLiteral(l: Any, t: Option[TypeInformation[_]] = None) extends ApiLeafExpression

case class ApiNull(resultType: TypeInformation[_]) extends ApiLeafExpression

case class ApiIn(expression: ApiExpression, elements: Seq[ApiExpression]) extends ApiExpression {
  override private[flink] def children: Seq[ApiExpression] = expression +: elements.distinct
}

case class ApiSymbolExpression(symbol: ApiTableSymbol) extends ApiLeafExpression

trait ApiTableSymbol

abstract class ApiTableSymbols extends Enumeration {
  class ApiTableSymbolValue extends Val() with ApiTableSymbol

  protected final def ApiValue = new ApiTableSymbolValue

  implicit def symbolToExpression(symbol: ApiTableSymbolValue): ApiSymbolExpression =
    ApiSymbolExpression(symbol)
}

object ApiTimeIntervalUnit extends ApiTableSymbols {
  type ApiTimeIntervalUnit = ApiTableSymbolValue

  val YEAR, YEAR_TO_MONTH, QUARTER, MONTH,
  WEEK, DAY, DAY_TO_HOUR, DAY_TO_MINUTE, DAY_TO_SECOND,
  HOUR, HOUR_TO_MINUTE, HOUR_TO_SECOND, MINUTE, MINUTE_TO_SECOND, SECOND = ApiValue

}

object ApiTimePointUnit extends ApiTableSymbols {
  type ApiTimePointUnit= ApiTableSymbolValue

  val YEAR, MONTH, DAY,
    HOUR, MINUTE, SECOND,
    QUARTER, WEEK, MILLISECOND, MICROSECOND = ApiValue
}

object ApiTrimMode extends ApiTableSymbols {
  type ApiTrimMode = ApiTableSymbolValue

  val BOTH, LEADING, TRAILING = ApiValue
}

object ApiTrimConstants {
  val TRIM_DEFAULT_CHAR = ApiLiteral(" ")
}

object ApiExpressionUtils {
  private[flink] def toMonthInterval(expr: ApiExpression, multiplier: Int): ApiExpression =
    expr match {
      case ApiLiteral(value: Int, None) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MONTHS))
      case ApiLiteral(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MONTHS))
      case _ =>
        ApiCast(
          ApiCall("times", Seq(expr, ApiLiteral(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: ApiExpression, multiplier: Long): ApiExpression =
    expr match {
      case ApiLiteral(value: Int, None) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case ApiLiteral(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case ApiLiteral(value: Long, None) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case ApiLiteral(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
        ApiLiteral(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case _ =>
        ApiCast(
          ApiCall("times", Seq(expr, ApiLiteral(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
    }

  private[flink] def toRowInterval(expr: ApiExpression): ApiExpression =
    expr match {
      case ApiLiteral(value: Int, None) =>
        ApiLiteral(value.toLong, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case ApiLiteral(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        ApiLiteral(value.toLong, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case ApiLiteral(value: Long, None) =>
        ApiLiteral(value, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case ApiLiteral(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
        ApiLiteral(value, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
    }

  private[flink] def convertArray(array: Array[_]): ApiExpression = {
    def createArray(): ApiExpression = {
      ApiCall("array", array.map(ApiLiteral(_)))
    }

    array match {
      // primitives
      case _: Array[Boolean] => createArray()
      case _: Array[Byte] => createArray()
      case _: Array[Short] => createArray()
      case _: Array[Int] => createArray()
      case _: Array[Long] => createArray()
      case _: Array[Float] => createArray()
      case _: Array[Double] => createArray()

      // boxed types
      case _: Array[JBoolean] => createArray()
      case _: Array[JByte] => createArray()
      case _: Array[JShort] => createArray()
      case _: Array[JInteger] => createArray()
      case _: Array[JLong] => createArray()
      case _: Array[JFloat] => createArray()
      case _: Array[JDouble] => createArray()

      // others
      case _: Array[String] => createArray()
      case _: Array[JBigDecimal] => createArray()
      case _: Array[Date] => createArray()
      case _: Array[Time] => createArray()
      case _: Array[Timestamp] => createArray()
      case bda: Array[BigDecimal] => ApiCall("array", bda.map { bd => ApiLiteral(bd.bigDecimal) })

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          ApiCall("array", array.map { na => convertArray(na.asInstanceOf[Array[_]]) })
        } else {
          throw new ValidationException("Unsupported array type.")
        }
    }
  }
}

