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
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.api.base.visitor.ExpressionVisitor
import org.apache.flink.table.typeutils.TypeCheckUtils.{isArray, isComparable, isNumeric}
import org.apache.flink.table.validate._

abstract class BinaryComparison extends BinaryExpression {

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if isComparable(lType) && lType == rType => ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(
          s"Comparison is only supported for numeric types and " +
            s"comparable types of same type, got $lType and $rType")
    }
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left === $right"

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
        ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Equality predicate on incompatible types: $lType and $rType")
    }

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class NotEqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left !== $right"

  override private[flink] def validateInput(): ValidationResult =
    (left.resultType, right.resultType) match {
      case (lType, rType) if isNumeric(lType) && isNumeric(rType) => ValidationSuccess
      case (lType, rType) if lType == rType => ValidationSuccess
      case (lType, rType) if isArray(lType) && lType.getTypeClass == rType.getTypeClass =>
        ValidationSuccess
      case (lType, rType) =>
        ValidationFailure(s"Inequality predicate on incompatible types: $lType and $rType")
    }

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left > $right"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left >= $right"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left < $right"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def toString = s"$left <= $right"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNull"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsNotNull(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNotNull"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsTrue(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isTrue"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsFalse(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isFalse"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsNotTrue(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNotTrue"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class IsNotFalse(child: Expression) extends UnaryExpression {
  override def toString = s"($child).isNotFalse"

  override private[flink] def resultType = BOOLEAN_TYPE_INFO

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

abstract class BetweenComparison(
    expr: Expression,
    lowerBound: Expression,
    upperBound: Expression)
  extends Expression {

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.BOOLEAN_TYPE_INFO

  override private[flink] def children: Seq[Expression] = Seq(expr, lowerBound, upperBound)

  override private[flink] def validateInput(): ValidationResult = {
    (expr.resultType, lowerBound.resultType, upperBound.resultType) match {
      case (exprType, lowerType, upperType)
          if isNumeric(exprType) && isNumeric(lowerType) && isNumeric(upperType) =>
        ValidationSuccess
      case (exprType, lowerType, upperType)
          if isComparable(exprType) && exprType == lowerType && exprType == upperType =>
        ValidationSuccess
      case (exprType, lowerType, upperType) =>
        ValidationFailure(
          s"Between is only supported for numeric types and " +
            s"identical comparable types, but got $exprType, $lowerType and $upperType"
        )
    }
  }
}

case class Between(
    expr: Expression,
    lowerBound: Expression,
    upperBound: Expression)
  extends BetweenComparison(expr, lowerBound, upperBound) {

  override def toString: String = s"($expr).between($lowerBound, $upperBound)"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

case class NotBetween(
    expr: Expression,
    lowerBound: Expression,
    upperBound: Expression)
  extends BetweenComparison(expr, lowerBound, upperBound) {

  override def toString: String = s"($expr).notBetween($lowerBound, $upperBound)"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}
