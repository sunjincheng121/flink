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

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.{Table, ValidationException}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.plan.TreeNode
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

abstract class Expression extends TreeNode[Expression]

abstract class BinaryExpression extends Expression {
  private[flink] def left: Expression
  private[flink] def right: Expression
  private[flink] def children = Seq(left, right)
}

abstract class UnaryExpression extends Expression {
  private[flink] def child: Expression
  private[flink] def children = Seq(child)
}

abstract class LeafExpression extends Expression {
  private[flink] val children = Nil
}

case class DistinctAgg(child: Expression) extends UnaryExpression

case class AggFunctionCall(
    aggregateFunction: AggregateFunction[_, _],
    resultTypeInfo: TypeInformation[_],
    accTypeInfo: TypeInformation[_],
    args: Seq[Expression])
  extends Expression {
  override def children: Seq[Expression] = args
}

case class Call(functionName: String, args: Seq[Expression]) extends Expression {
  override def children: Seq[Expression] = args
}

case class UnresolvedOverCall(agg: Expression, alias: Expression)
  extends Expression {
  override private[flink] def children: Seq[Expression] = Seq(agg, alias)
}

case class ScalarFunctionCall(
    scalarFunction: ScalarFunction,
    parameters: Seq[Expression])
  extends Expression {
  override private[flink] def children: Seq[Expression] = parameters
}

case class TableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    resultType: TypeInformation[_])
  extends Expression {
  override private[flink] def children: Seq[Expression] = parameters
}

case class Cast(child: Expression, resultType: TypeInformation[_]) extends UnaryExpression

case class Flattening(child: Expression) extends UnaryExpression

case class GetCompositeField(child: Expression, key: Any) extends UnaryExpression

case class UnresolvedFieldReference(name: String) extends LeafExpression

case class Alias(child: Expression, name: String, extraNames: Seq[String] = Seq())
  extends UnaryExpression

case class TableReference(name: String, table: Table) extends LeafExpression

case class RowtimeAttribute(expr: Expression) extends UnaryExpression {
  override private[flink] def child: Expression = expr
}

case class ProctimeAttribute(expr: Expression) extends UnaryExpression {
  override private[flink] def child: Expression = expr
}

case class StreamRecordTimestamp() extends LeafExpression

case class Literal(l: Any, t: Option[TypeInformation[_]] = None) extends LeafExpression

case class Null(resultType: TypeInformation[_]) extends LeafExpression

case class In(expression: Expression, elements: Seq[Expression]) extends Expression {
  override private[flink] def children: Seq[Expression] = expression +: elements.distinct
}

case class SymbolExpression(symbol: TableSymbol) extends LeafExpression

trait TableSymbol

abstract class TableSymbols extends Enumeration {
  class TableSymbolValue extends Val() with TableSymbol

  protected final def SymbolValue = new TableSymbolValue

  implicit def symbolToExpression(symbol: TableSymbolValue): SymbolExpression =
    SymbolExpression(symbol)
}

object TimeIntervalUnit extends TableSymbols {
  type TimeIntervalUnit = TableSymbolValue

  val YEAR, YEAR_TO_MONTH, QUARTER, MONTH,
  WEEK, DAY, DAY_TO_HOUR, DAY_TO_MINUTE, DAY_TO_SECOND,
  HOUR, HOUR_TO_MINUTE, HOUR_TO_SECOND, MINUTE, MINUTE_TO_SECOND, SECOND = SymbolValue

}

object TimePointUnit extends TableSymbols {
  type TimePointUnit= TableSymbolValue

  val YEAR, MONTH, DAY,
    HOUR, MINUTE, SECOND,
    QUARTER, WEEK, MILLISECOND, MICROSECOND = SymbolValue
}

object TrimMode extends TableSymbols {
  type TrimMode = TableSymbolValue

  val BOTH, LEADING, TRAILING = SymbolValue
}

object TrimConstants {
  val TRIM_DEFAULT_CHAR = Literal(" ")
}

object ExpressionUtils {
  private[flink] def toMonthInterval(expr: Expression, multiplier: Int): Expression =
    expr match {
      case Literal(value: Int, None) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MONTHS))
      case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MONTHS))
      case _ =>
        Cast(
          Call("times", Seq(expr, Literal(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MONTHS)
  }

  private[flink] def toMilliInterval(expr: Expression, multiplier: Long): Expression =
    expr match {
      case Literal(value: Int, None) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case Literal(value: Long, None) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case Literal(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
        Literal(value * multiplier, Some(TimeIntervalTypeInfo.INTERVAL_MILLIS))
      case _ =>
        Cast(
          Call("times", Seq(expr, Literal(multiplier))),
          TimeIntervalTypeInfo.INTERVAL_MILLIS)
    }

  private[flink] def toRowInterval(expr: Expression): Expression =
    expr match {
      case Literal(value: Int, None) =>
        Literal(value.toLong, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case Literal(value: Int, BasicTypeInfo.INT_TYPE_INFO) =>
        Literal(value.toLong, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case Literal(value: Long, None) =>
        Literal(value, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
      case Literal(value: Long, BasicTypeInfo.LONG_TYPE_INFO) =>
        Literal(value, Some(RowIntervalTypeInfo.INTERVAL_ROWS))
    }

  private[flink] def convertArray(array: Array[_]): Expression = {
    def createArray(): Expression = {
      Call("array", array.map(Literal(_)))
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
      case bda: Array[BigDecimal] => Call("array", bda.map { bd => Literal(bd.bigDecimal) })

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          Call("array", array.map { na => convertArray(na.asInstanceOf[Array[_]]) })
        } else {
          throw new ValidationException("Unsupported array type.")
        }
    }
  }
}

