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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.MultisetTypeInfo
import org.apache.flink.table.api.base.visitor.{AggregationVisitor, ExpressionVisitor}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract sealed class Aggregation extends Expression {

  override def toString = s"Aggregate"

  def accept[T](visitor: AggregationVisitor[T]): T

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    throw new UnsupportedOperationException("Aggregate cannot be transformed to RexNode")
}

case class DistinctAgg(child: Expression) extends Aggregation {

  def distinct: Expression = DistinctAgg(child)

  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult = {
    super.validateInput()
    child match {
      case agg: Aggregation =>
        child.validateInput()
      case _ =>
        ValidationFailure(s"Distinct modifier cannot be applied to $child! " +
          s"It can only be applied to an aggregation expression, for example, " +
          s"'a.count.distinct which is equivalent with COUNT(DISTINCT a).")
    }
  }

  override private[flink] def children = Seq(child)

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class Sum(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class Sum0(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"sum0($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sum0")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class Min(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"min($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "min")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class Max(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"max($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertOrderableExpr(child.resultType, "max")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class Count(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"count($child)"

  override private[flink] def resultType = BasicTypeInfo.LONG_TYPE_INFO

  override def accept[T](visitor: AggregationVisitor[T]): T = {
    visitor.visit(this)
  }
}

case class Avg(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"avg($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "avg")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

/**
  * Returns a multiset aggregates.
  */
case class Collect(child: Expression) extends Aggregation  {

  override private[flink] def children: Seq[Expression] = Seq(child)

  override private[flink] def resultType: TypeInformation[_] =
    MultisetTypeInfo.getInfoFor(child.resultType)

  override def toString: String = s"collect($child)"

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class StddevPop(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"stddev_pop($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "stddev_pop")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class StddevSamp(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"stddev_samp($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "stddev_samp")


  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class VarPop(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"var_pop($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "var_pop")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class VarSamp(child: Expression) extends Aggregation {
  override private[flink] def children: Seq[Expression] = Seq(child)
  override def toString = s"var_samp($child)"

  override private[flink] def resultType = child.resultType

  override private[flink] def validateInput() =
    TypeCheckUtils.assertNumericExpr(child.resultType, "var_samp")

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)
}

case class AggFunctionCall(
    aggregateFunction: AggregateFunction[_, _],
    resultTypeInfo: TypeInformation[_],
    accTypeInfo: TypeInformation[_],
    args: Seq[Expression])
  extends Aggregation {

  override private[flink] def children: Seq[Expression] = args

  override def resultType: TypeInformation[_] = resultTypeInfo

  override def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    val foundSignature = getAccumulateMethodSignature(aggregateFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${signatureToString(signature)} \n" +
                          s"Expected: ${
                            getMethodSignatures(aggregateFunction, "accumulate")
                              .map(_.drop(1))
                              .map(signatureToString)
                              .mkString(", ")}")
    } else {
      ValidationSuccess
    }
  }

  override def toString: String = s"${aggregateFunction.getClass.getSimpleName}($args)"

  override def accept[T](visitor: AggregationVisitor[T]): T = visitor.visit(this)

  override def accept[T](visitor: ExpressionVisitor[T]): T = visitor.visit(this)
}
