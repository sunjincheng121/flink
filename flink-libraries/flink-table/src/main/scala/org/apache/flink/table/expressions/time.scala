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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.base.visitor.ExpressionVisitor
import org.apache.flink.table.expressions.TimeIntervalUnit.TimeIntervalUnit
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.{TimeIntervalTypeInfo, TypeCheckUtils}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

case class Extract(timeIntervalUnit: Expression, temporal: Expression) extends Expression {

  override private[flink] def children: Seq[Expression] = timeIntervalUnit :: temporal :: Nil

  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isTemporal(temporal.resultType)) {
      return ValidationFailure(s"Extract operator requires Temporal input, " +
        s"but $temporal is of type ${temporal.resultType}")
    }

    timeIntervalUnit match {
      case SymbolExpression(TimeIntervalUnit.YEAR)
           | SymbolExpression(TimeIntervalUnit.QUARTER)
           | SymbolExpression(TimeIntervalUnit.MONTH)
           | SymbolExpression(TimeIntervalUnit.WEEK)
           | SymbolExpression(TimeIntervalUnit.DAY)
        if temporal.resultType == SqlTimeTypeInfo.DATE
          || temporal.resultType == SqlTimeTypeInfo.TIMESTAMP
          || temporal.resultType == TimeIntervalTypeInfo.INTERVAL_MILLIS
          || temporal.resultType == TimeIntervalTypeInfo.INTERVAL_MONTHS =>
        ValidationSuccess

      case SymbolExpression(TimeIntervalUnit.HOUR)
           | SymbolExpression(TimeIntervalUnit.MINUTE)
           | SymbolExpression(TimeIntervalUnit.SECOND)
        if temporal.resultType == SqlTimeTypeInfo.TIME
          || temporal.resultType == SqlTimeTypeInfo.TIMESTAMP
          || temporal.resultType == TimeIntervalTypeInfo.INTERVAL_MILLIS =>
        ValidationSuccess

      case _ =>
        ValidationFailure(s"Extract operator does not support unit '$timeIntervalUnit' for input" +
          s" of type '${temporal.resultType}'.")
    }
  }

  override def toString: String = s"($temporal).extract($timeIntervalUnit)"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

abstract class TemporalCeilFloor(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends Expression {

  override private[flink] def children: Seq[Expression] = timeIntervalUnit :: temporal :: Nil

  override private[flink] def resultType: TypeInformation[_] = temporal.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isTimePoint(temporal.resultType)) {
      return ValidationFailure(s"Temporal ceil/floor operator requires Time Point input, " +
        s"but $temporal is of type ${temporal.resultType}")
    }
    val unit = timeIntervalUnit match {
      case SymbolExpression(u: TimeIntervalUnit) => Some(u)
      case _ => None
    }
    if (unit.isEmpty) {
      return ValidationFailure(s"Temporal ceil/floor operator requires Time Interval Unit " +
        s"input, but $timeIntervalUnit is of type ${timeIntervalUnit.resultType}")
    }

    (unit.get, temporal.resultType) match {
      case (TimeIntervalUnit.YEAR | TimeIntervalUnit.MONTH,
          SqlTimeTypeInfo.DATE | SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case (TimeIntervalUnit.DAY, SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case (TimeIntervalUnit.HOUR | TimeIntervalUnit.MINUTE | TimeIntervalUnit.SECOND,
          SqlTimeTypeInfo.TIME | SqlTimeTypeInfo.TIMESTAMP) =>
        ValidationSuccess
      case _ =>
        ValidationFailure(s"Temporal ceil/floor operator does not support " +
          s"unit '$timeIntervalUnit' for input of type '${temporal.resultType}'.")
    }
  }
}

case class TemporalFloor(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends TemporalCeilFloor(
    timeIntervalUnit,
    temporal) {

  override def toString: String = s"($temporal).floor($timeIntervalUnit)"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class TemporalCeil(
    timeIntervalUnit: Expression,
    temporal: Expression)
  extends TemporalCeilFloor(
    timeIntervalUnit,
    temporal) {

  override def toString: String = s"($temporal).ceil($timeIntervalUnit)"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

abstract class CurrentTimePoint(
    targetType: TypeInformation[_],
    local: Boolean)
  extends LeafExpression {

  override private[flink] def resultType: TypeInformation[_] = targetType

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isTimePoint(targetType)) {
      ValidationFailure(s"CurrentTimePoint operator requires Time Point target type, " +
        s"but get $targetType.")
    } else if (local && targetType == SqlTimeTypeInfo.DATE) {
      ValidationFailure(s"Localized CurrentTimePoint operator requires Time or Timestamp target " +
        s"type, but get $targetType.")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = if (local) {
    s"local$targetType()"
  } else {
    s"current$targetType()"
  }
}

case class CurrentDate() extends CurrentTimePoint(SqlTimeTypeInfo.DATE, local = false) {

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class CurrentTime() extends CurrentTimePoint(SqlTimeTypeInfo.TIME, local = false) {

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class CurrentTimestamp() extends CurrentTimePoint(SqlTimeTypeInfo.TIMESTAMP, local = false) {

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class LocalTime() extends CurrentTimePoint(SqlTimeTypeInfo.TIME, local = true) {

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class LocalTimestamp() extends CurrentTimePoint(SqlTimeTypeInfo.TIMESTAMP, local = true) {

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

/**
  * Extracts the quarter of a year from a SQL date.
  */
case class Quarter(child: Expression) extends UnaryExpression with InputTypeSpec {

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = Seq(SqlTimeTypeInfo.DATE)

  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override def toString: String = s"($child).quarter()"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

/**
  * Determines whether two anchored time intervals overlap.
  */
case class TemporalOverlaps(
    leftTimePoint: Expression,
    leftTemporal: Expression,
    rightTimePoint: Expression,
    rightTemporal: Expression)
  extends Expression {

  override private[flink] def children: Seq[Expression] =
    Seq(leftTimePoint, leftTemporal, rightTimePoint, rightTemporal)

  override private[flink] def resultType: TypeInformation[_] = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isTimePoint(leftTimePoint.resultType)) {
      return ValidationFailure(s"TemporalOverlaps operator requires leftTimePoint to be of type " +
        s"Time Point, but get ${leftTimePoint.resultType}.")
    }
    if (!TypeCheckUtils.isTimePoint(rightTimePoint.resultType)) {
      return ValidationFailure(s"TemporalOverlaps operator requires rightTimePoint to be of " +
        s"type Time Point, but get ${rightTimePoint.resultType}.")
    }
    if (leftTimePoint.resultType != rightTimePoint.resultType) {
      return ValidationFailure(s"TemporalOverlaps operator requires leftTimePoint and " +
        s"rightTimePoint to be of same type.")
    }

    // leftTemporal is point, then it must be comparable with leftTimePoint
    if (TypeCheckUtils.isTimePoint(leftTemporal.resultType)) {
      if (leftTemporal.resultType != leftTimePoint.resultType) {
        return ValidationFailure(s"TemporalOverlaps operator requires leftTemporal and " +
          s"leftTimePoint to be of same type if leftTemporal is of type Time Point.")
      }
    } else if (!isTimeInterval(leftTemporal.resultType)) {
      return ValidationFailure(s"TemporalOverlaps operator requires leftTemporal to be of " +
        s"type Time Point or Time Interval.")
    }

    // rightTemporal is point, then it must be comparable with rightTimePoint
    if (TypeCheckUtils.isTimePoint(rightTemporal.resultType)) {
      if (rightTemporal.resultType != rightTimePoint.resultType) {
        return ValidationFailure(s"TemporalOverlaps operator requires rightTemporal and " +
          s"rightTimePoint to be of same type if rightTemporal is of type Time Point.")
      }
    } else if (!isTimeInterval(rightTemporal.resultType)) {
      return ValidationFailure(s"TemporalOverlaps operator requires rightTemporal to be of " +
        s"type Time Point or Time Interval.")
    }
    ValidationSuccess
  }

  override def toString: String = s"temporalOverlaps(${children.mkString(", ")})"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class DateFormat(timestamp: Expression, format: Expression) extends Expression {
  override private[flink] def children = timestamp :: format :: Nil

  override def toString: String = s"$timestamp.dateFormat($format)"

  override private[flink] def resultType = STRING_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class TimestampDiff(
    timePointUnit: Expression,
    timePoint1: Expression,
    timePoint2: Expression)
  extends Expression {

  override private[flink] def children: Seq[Expression] =
    timePointUnit :: timePoint1 :: timePoint2 :: Nil

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isTimePoint(timePoint1.resultType)) {
      return ValidationFailure(
        s"$this requires an input time point type, " +
        s"but timePoint1 is of type '${timePoint1.resultType}'.")
    }

    if (!TypeCheckUtils.isTimePoint(timePoint2.resultType)) {
      return ValidationFailure(
        s"$this requires an input time point type, " +
        s"but timePoint2 is of type '${timePoint2.resultType}'.")
    }

    timePointUnit match {
      case SymbolExpression(TimePointUnit.YEAR)
           | SymbolExpression(TimePointUnit.QUARTER)
           | SymbolExpression(TimePointUnit.MONTH)
           | SymbolExpression(TimePointUnit.WEEK)
           | SymbolExpression(TimePointUnit.DAY)
           | SymbolExpression(TimePointUnit.HOUR)
           | SymbolExpression(TimePointUnit.MINUTE)
           | SymbolExpression(TimePointUnit.SECOND)
        if timePoint1.resultType == SqlTimeTypeInfo.DATE
          || timePoint1.resultType == SqlTimeTypeInfo.TIMESTAMP
          || timePoint2.resultType == SqlTimeTypeInfo.DATE
          || timePoint2.resultType == SqlTimeTypeInfo.TIMESTAMP =>
        ValidationSuccess

      case _ =>
        ValidationFailure(s"$this operator does not support unit '$timePointUnit'" +
            s" for input of type ('${timePoint1.resultType}', '${timePoint2.resultType}').")
    }
  }

  override def toString: String = s"timestampDiff(${children.mkString(", ")})"

  override private[flink] def resultType = INT_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}
