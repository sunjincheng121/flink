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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.typeutils.TypeCheckUtils
import org.apache.flink.table.validate._

import scala.collection.JavaConversions._

case class Abs(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Abs")

  override def toString: String = s"abs($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ABS, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]): R = {
    visitor.visitCall(PlannerCall("abs", children))
  }
}

case class Ceil(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Ceil")

  override def toString: String = s"ceil($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.CEIL, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("ceil", children))
  }
}

case class Exp(child: PlannerExpression) extends UnaryPlannerExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"exp($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.EXP, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("exp", children))
  }
}


case class Floor(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = LONG_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Floor")

  override def toString: String = s"floor($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.FLOOR, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("floor", children))
  }
}

case class Log10(child: PlannerExpression) extends UnaryPlannerExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"log10($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LOG10, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("log10", children))
  }
}

case class Log2(child: PlannerExpression) extends UnaryPlannerExpression with InputTypeSpec {
  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.LOG2, child.toRexNode)
  }

  override def toString: String = s"log2($child)"

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("log2", children))
  }
}

case class Cosh(child: PlannerExpression) extends UnaryPlannerExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.COSH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cosh")

  override def toString = s"cosh($child)"

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("cosh", children))
  }
}

case class Log(base: PlannerExpression, antilogarithm: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {
  def this(antilogarithm: PlannerExpression) = this(E(), antilogarithm)

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def children: Seq[PlannerExpression] =
    if (base == null) Seq(antilogarithm) else Seq(base, antilogarithm)

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq.fill(children.length)(DOUBLE_TYPE_INFO)

  override def toString: String = s"log(${children.mkString(",")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.LOG, children.map(_.toRexNode))
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("log", children))
  }
}

object Log {
  def apply(antilogarithm: PlannerExpression): Log = Log(null, antilogarithm)
}

case class Ln(child: PlannerExpression) extends UnaryPlannerExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"ln($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.LN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("ln", children))
  }
}

case class Power(left: PlannerExpression, right: PlannerExpression)
  extends BinaryPlannerExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    DOUBLE_TYPE_INFO :: DOUBLE_TYPE_INFO :: Nil

  override def toString: String = s"pow($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, left.toRexNode, right.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("power", children))
  }
}

case class Sinh(child: PlannerExpression) extends UnaryPlannerExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO;

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Sinh")

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.SINH, child.toRexNode)
  }

  override def toString = s"sinh($child)"

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("sinh", children))
  }
}

case class Sqrt(child: PlannerExpression) extends UnaryPlannerExpression with InputTypeSpec {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    Seq(DOUBLE_TYPE_INFO)

  override def toString: String = s"sqrt($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.POWER, child.toRexNode, PlannerLiteral(0.5).toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("sqrt", children))
  }
}

case class Sin(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Sin")

  override def toString: String = s"sin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("sin", children))
  }
}

case class Cos(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cos")

  override def toString: String = s"cos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.COS, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("cos", children))
  }
}

case class Tan(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Tan")

  override def toString: String = s"tan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.TAN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("tan", children))
  }
}

case class Tanh(child: PlannerExpression) extends UnaryPlannerExpression {

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder) = {
    relBuilder.call(ScalarSqlFunctions.TANH, child.toRexNode)
  }

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Tanh")

  override def toString = s"tanh($child)"

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("tanh", children))
  }
}

case class Cot(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Cot")

  override def toString: String = s"cot($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.COT, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("cot", children))
  }
}

case class Asin(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Asin")

  override def toString: String = s"asin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ASIN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("asin", children))
  }
}

case class Acos(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Acos")

  override def toString: String = s"acos($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ACOS, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("acos", children))
  }
}

case class Atan(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Atan")

  override def toString: String = s"atan($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ATAN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("atan", children))
  }
}

case class Atan2(y: PlannerExpression, x: PlannerExpression) extends BinaryPlannerExpression {

  override private[flink] def left = y

  override private[flink] def right = x

  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput() = {
    TypeCheckUtils.assertNumericExpr(y.resultType, "atan2")
    TypeCheckUtils.assertNumericExpr(x.resultType, "atan2")
  }

  override def toString: String = s"atan2($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ATAN2, left.toRexNode, right.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("atan2", children))
  }
}

case class Degrees(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Degrees")

  override def toString: String = s"degrees($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.DEGREES, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("degrees", children))
  }
}

case class Radians(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "Radians")

  override def toString: String = s"radians($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RADIANS, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("radians", children))
  }
}

case class Sign(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = child.resultType

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertNumericExpr(child.resultType, "sign")

  override def toString: String = s"sign($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.SIGN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("sign", children))
  }
}

case class Round(left: PlannerExpression, right: PlannerExpression)
  extends BinaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = left.resultType

  override private[flink] def validateInput(): ValidationResult = {
    if (!TypeCheckUtils.isInteger(right.resultType)) {
      ValidationFailure(s"round right requires int, get " +
        s"$right : ${right.resultType}")
    }
    TypeCheckUtils.assertNumericExpr(left.resultType, s"round left :$left")
  }

  override def toString: String = s"round($left, $right)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.ROUND, left.toRexNode, right.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("round", children))
  }
}

case class Pi() extends LeafPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override def toString: String = s"pi()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.PI)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("pi", children))
  }
}

case class E() extends LeafPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = DOUBLE_TYPE_INFO

  override def toString: String = s"e()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.E)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("e", children))
  }
}

case class Rand(seed: PlannerExpression) extends PlannerExpression with InputTypeSpec {

  def this() = this(null)

  override private[flink] def children: Seq[PlannerExpression] = if (seed != null) {
    seed :: Nil
  } else {
    Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.DOUBLE_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: Nil
  } else {
    Nil
  }

  override def toString: String = if (seed != null) {
    s"rand($seed)"
  } else {
    s"rand()"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND, children.map(_.toRexNode))
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("rand", children))
  }
}

case class RandInteger(seed: PlannerExpression, bound: PlannerExpression)
  extends PlannerExpression with InputTypeSpec {

  def this(bound: PlannerExpression) = this(null, bound)

  override private[flink] def children: Seq[PlannerExpression] = if (seed != null) {
    seed :: bound :: Nil
  } else {
    bound :: Nil
  }

  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.INT_TYPE_INFO

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] = if (seed != null) {
    INT_TYPE_INFO :: INT_TYPE_INFO :: Nil
  } else {
    INT_TYPE_INFO :: Nil
  }

  override def toString: String = if (seed != null) {
    s"randInteger($seed, $bound)"
  } else {
    s"randInteger($bound)"
  }

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.RAND_INTEGER, children.map(_.toRexNode))
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("randInteger", children))
  }
}

case class Bin(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult =
    TypeCheckUtils.assertIntegerFamilyExpr(child.resultType, "Bin")

  override def toString: String = s"bin($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.BIN, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("bin", children))
  }
}

case class Hex(child: PlannerExpression) extends UnaryPlannerExpression {
  override private[flink] def resultType: TypeInformation[_] = BasicTypeInfo.STRING_TYPE_INFO

  override private[flink] def validateInput(): ValidationResult = {
    if (TypeCheckUtils.isIntegerFamily(child.resultType) ||
        TypeCheckUtils.isString(child.resultType)) {
      ValidationSuccess
    } else {
      ValidationFailure(s"hex() requires an integer or string input but was '${child.resultType}'.")
    }
  }

  override def toString: String = s"hex($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.HEX, child.toRexNode)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("hex", children))
  }
}

case class UUID() extends LeafPlannerExpression {
  override private[flink] def resultType = BasicTypeInfo.STRING_TYPE_INFO

  override def toString: String = s"uuid()"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(ScalarSqlFunctions.UUID)
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("uuid", children))
  }
}

case class Truncate(base: PlannerExpression, num: PlannerExpression)
    extends PlannerExpression with InputTypeSpec {
  def this(base: PlannerExpression) = this(base, null)

  override private[flink] def resultType: TypeInformation[_] = base.resultType

  override private[flink] def children: Seq[PlannerExpression] =
    if (num == null) Seq(base) else Seq(base, num)

  override private[flink] def expectedTypes: Seq[TypeInformation[_]] =
    if (num == null) Seq(base.resultType) else Seq(base.resultType, INT_TYPE_INFO)

  override def toString: String = s"truncate(${children.mkString(",")})"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    relBuilder.call(SqlStdOperatorTable.TRUNCATE, children.map(_.toRexNode))
  }

  override private[flink] def validateInput(): ValidationResult = {
    if (num != null) {
      if (!TypeCheckUtils.isInteger(num.resultType)) {
        ValidationFailure(s"truncate num requires int, get " +
          s"$num : ${num.resultType}")
      }
    }
    TypeCheckUtils.assertNumericExpr(base.resultType, s"truncate base :$base")
  }

  override private[flink] def accept[R](visitor: PlannerExpressionVisitor[R]) = {
    visitor.visitCall(PlannerCall("truncate", children))
  }
}

object Truncate {
  def apply(base: PlannerExpression): Truncate = Truncate(base, null)
}

