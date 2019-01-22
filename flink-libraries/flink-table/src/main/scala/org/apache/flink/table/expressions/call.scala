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
import org.apache.flink.table.api._
import org.apache.flink.table.api.base.visitor.ExpressionVisitor
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions._
import org.apache.flink.table.plan.logical.{LogicalNode, LogicalTableFunctionCall}
import org.apache.flink.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}
import org.apache.flink.table.typeutils.{RowIntervalTypeInfo, TimeIntervalTypeInfo}

/**
  * General expression for unresolved function calls. The function can be a built-in
  * scalar function or a user-defined scalar function.
  */
case class Call(functionName: String, args: Seq[Expression]) extends Expression {

  override private[flink] def children: Seq[Expression] = args

  override def toString = s"\\$functionName(${args.mkString(", ")})"

  override private[flink] def resultType =
    throw UnresolvedException(s"calling resultType on UnresolvedFunction $functionName")

  override private[flink] def validateInput(): ValidationResult =
    ValidationFailure(s"Unresolved function call: $functionName")

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    throw UnresolvedException(s"trying to convert UnresolvedFunction $functionName to RexNode")
}

/**
  * Over call with unresolved alias for over window.
  *
  * @param agg The aggregation of the over call.
  * @param alias The alias of the referenced over window.
  */
case class UnresolvedOverCall(agg: Expression, alias: Expression) extends Expression {

  override private[flink] def validateInput() =
    ValidationFailure(s"Over window with alias $alias could not be resolved.")

  override private[flink] def resultType = agg.resultType

  override private[flink] def children = Seq()

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    throwUnsupportedToRexNodeOperationException
}

/**
  * Over expression for Calcite over transform.
  *
  * @param agg            over-agg expression
  * @param partitionBy    The fields by which the over window is partitioned
  * @param orderBy        The field by which the over window is sorted
  * @param preceding      The lower bound of the window
  * @param following      The upper bound of the window
  */
case class OverCall(
    agg: Expression,
    partitionBy: Seq[Expression],
    orderBy: Expression,
    preceding: Expression,
    following: Expression) extends Expression {

  override def toString: String = s"$agg OVER (" +
    s"PARTITION BY (${partitionBy.mkString(", ")}) " +
    s"ORDER BY $orderBy " +
    s"PRECEDING $preceding " +
    s"FOLLOWING $following)"

  override private[flink] def children: Seq[Expression] =
    Seq(agg) ++ Seq(orderBy) ++ partitionBy ++ Seq(preceding) ++ Seq(following)

  override private[flink] def resultType = agg.resultType

  override private[flink] def validateInput(): ValidationResult = {

    // check that agg expression is aggregation
    agg match {
      case _: Aggregation =>
        ValidationSuccess
      case _ =>
        return ValidationFailure(s"OVER can only be applied on an aggregation.")
    }

    // check partitionBy expression keys are resolved field reference
    partitionBy.foreach {
      case r: ResolvedFieldReference if r.resultType.isKeyType  =>
        ValidationSuccess
      case r: ResolvedFieldReference =>
        return ValidationFailure(s"Invalid PartitionBy expression: $r. " +
          s"Expression must return key type.")
      case r =>
        return ValidationFailure(s"Invalid PartitionBy expression: $r. " +
          s"Expression must be a resolved field reference.")
    }

    // check preceding is valid
    preceding match {
      case _: CurrentRow | _: CurrentRange | _: UnboundedRow | _: UnboundedRange =>
        ValidationSuccess
      case Literal(v: Long, _: RowIntervalTypeInfo) if v > 0 =>
        ValidationSuccess
      case Literal(_, _: RowIntervalTypeInfo) =>
        return ValidationFailure("Preceding row interval must be larger than 0.")
      case Literal(v: Long, _: TimeIntervalTypeInfo[_]) if v >= 0 =>
        ValidationSuccess
      case Literal(_, _: TimeIntervalTypeInfo[_]) =>
        return ValidationFailure("Preceding time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Preceding must be a row interval or time interval literal.")
    }

    // check following is valid
    following match {
      case _: CurrentRow | _: CurrentRange | _: UnboundedRow | _: UnboundedRange =>
        ValidationSuccess
      case Literal(v: Long, _: RowIntervalTypeInfo) if v > 0 =>
        ValidationSuccess
      case Literal(_, _: RowIntervalTypeInfo) =>
        return ValidationFailure("Following row interval must be larger than 0.")
      case Literal(v: Long, _: TimeIntervalTypeInfo[_]) if v >= 0 =>
        ValidationSuccess
      case Literal(_, _: TimeIntervalTypeInfo[_]) =>
        return ValidationFailure("Following time interval must be equal or larger than 0.")
      case Literal(_, _) =>
        return ValidationFailure("Following must be a row interval or time interval literal.")
    }

    // check that preceding and following are of same type
    (preceding, following) match {
      case (p: Expression, f: Expression) if p.resultType == f.resultType =>
        ValidationSuccess
      case _ =>
        return ValidationFailure("Preceding and following must be of same interval type.")
    }

    // check time field
    if (!ExpressionUtils.isTimeAttribute(orderBy)) {
      return ValidationFailure("Ordering must be defined on a time attribute.")
    }

    ValidationSuccess
  }

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

/**
  * Expression for calling a user-defined scalar functions.
  *
  * @param scalarFunction scalar function to be called (might be overloaded)
  * @param parameters actual parameters that determine target evaluation method
  */
case class ScalarFunctionCall(
    scalarFunction: ScalarFunction,
    parameters: Seq[Expression])
  extends Expression {

  private var foundSignature: Option[Array[Class[_]]] = None

  override private[flink] def children: Seq[Expression] = parameters

  override def toString =
    s"${scalarFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override private[flink] def resultType =
    getResultTypeOfScalarFunction(
      scalarFunction,
      foundSignature.get)

  override private[flink] def validateInput(): ValidationResult = {
    val signature = children.map(_.resultType)
    // look for a signature that matches the input types
    foundSignature = getEvalMethodSignature(scalarFunction, signature)
    if (foundSignature.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
        s"Actual: ${signatureToString(signature)} \n" +
        s"Expected: ${signaturesToString(scalarFunction, "eval")}")
    } else {
      ValidationSuccess
    }
  }

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    visitor.visit(this)
}

/**
  *
  * Expression for calling a user-defined table function with actual parameters.
  *
  * @param functionName function name
  * @param tableFunction user-defined table function
  * @param parameters actual parameters of function
  * @param resultType type information of returned table
  */
case class TableFunctionCall(
    functionName: String,
    tableFunction: TableFunction[_],
    parameters: Seq[Expression],
    resultType: TypeInformation[_])
  extends Expression {

  private var aliases: Option[Seq[String]] = None

  override private[flink] def children: Seq[Expression] = parameters

  /**
    * Assigns an alias for this table function's returned fields that the following operator
    * can refer to.
    *
    * @param aliasList alias for this table function's returned fields
    * @return this table function call
    */
  private[flink] def as(aliasList: Option[Seq[String]]): TableFunctionCall = {
    this.aliases = aliasList
    this
  }

  /**
    * Converts an API class to a logical node for planning.
    */
  private[flink] def toLogicalTableFunctionCall(child: LogicalNode): LogicalTableFunctionCall = {
    val originNames = getFieldInfo(resultType)._1

    // determine the final field names
    val fieldNames = if (aliases.isDefined) {
      val aliasList = aliases.get
      if (aliasList.length != originNames.length) {
        throw new ValidationException(
          s"List of column aliases must have same degree as table; " +
            s"the returned table of function '$functionName' has ${originNames.length} " +
            s"columns (${originNames.mkString(",")}), " +
            s"whereas alias list has ${aliasList.length} columns")
      } else {
        aliasList.toArray
      }
    } else {
      originNames
    }

    LogicalTableFunctionCall(
      functionName,
      tableFunction,
      parameters,
      resultType,
      fieldNames,
      child)
  }

  override def toString =
    s"${tableFunction.getClass.getCanonicalName}(${parameters.mkString(", ")})"

  override private[flink] def accept[T](visitor: ExpressionVisitor[T]): T =
    throwUnsupportedToRexNodeOperationException
}
