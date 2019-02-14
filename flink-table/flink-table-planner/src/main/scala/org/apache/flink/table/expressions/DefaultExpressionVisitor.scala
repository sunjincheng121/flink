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
import org.apache.flink.table.plan.expressions._
import org.apache.flink.table.plan.expressions.{Call => PlannerCall, Literal => PlannerLiteral, DistinctAgg => PlannerDistinctAgg, TableFunctionCall => PlannerTableFunctionCall, TableReference => PlannerTableReference}

import _root_.scala.collection.JavaConverters._

/**
  * Default implementation of expression visitor.
  */
class DefaultExpressionVisitor private extends ExpressionVisitor[PlannerExpression] {

  override def visitCall(call: Call): PlannerExpression = {

    val func = call.getFunctionDefinition
    val args = call.getChildren.asScala

    func match {
      case e: ScalarFunctionDefinition =>
        ScalarFunctionCall(e.getScalarFunction, args.map(_.accept(this)))

      case e: TableFunctionDefinition =>
        val tfc = PlannerTableFunctionCall(
          e.getName, e.getTableFunction, args.map(_.accept(this)), e.getResultType)
        val alias = call.asInstanceOf[TableFunctionCall].alias()
        if (alias.isPresent) {
          tfc.setAliases(alias.get())
        }
        tfc

      case aggFuncDef: AggregateFunctionDefinition =>
        AggFunctionCall(
          aggFuncDef.getAggregateFunction,
          aggFuncDef.getResultTypeInfo,
          aggFuncDef.getAccumulatorTypeInfo,
          args.map(_.accept(this)))

      case e: FunctionDefinition =>
        e match {
          case FunctionDefinitions.CAST =>
            assert(args.size == 2)
            Cast(args.head.accept(this), args.last.asInstanceOf[TypeLiteral].getType)

          case FunctionDefinitions.AS =>
            assert(args.size >= 2)
            val child = args(0)
            val name = args(1).asInstanceOf[Literal].getValue.asInstanceOf[String]
            val extraNames =
              args.drop(1).drop(1).map(e => e.asInstanceOf[Literal].getValue.asInstanceOf[String])
            Alias(child.accept(this), name, extraNames)

          case FunctionDefinitions.FLATTEN =>
            assert(args.size == 1)
            Flattening(args.head.accept(this))

          case FunctionDefinitions.GET_COMPOSITE_FIELD =>
            assert(args.size == 2)
            GetCompositeField(args.head.accept(this),
              args.last.asInstanceOf[Literal].getValue)

          case FunctionDefinitions.AND =>
            assert(args.size == 2)
            And(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.OR =>
            assert(args.size == 2)
            Or(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.NOT =>
            assert(args.size == 1)
            Not(args.head.accept(this))

          case FunctionDefinitions.EQUALS =>
            assert(args.size == 2)
            EqualTo(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.GREATER_THAN =>
            assert(args.size == 2)
            GreaterThan(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.GREATER_THAN_OR_EQUAL =>
            assert(args.size == 2)
            GreaterThanOrEqual(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.LESS_THAN =>
            assert(args.size == 2)
            LessThan(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.LESS_THAN_OR_EQUAL =>
            assert(args.size == 2)
            LessThanOrEqual(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.NOT_EQUALS =>
            assert(args.size == 2)
            NotEqualTo(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.IN =>
            In(args.head.accept(this), args.slice(1, args.size).map(_.accept(this)))

          case FunctionDefinitions.IS_NULL =>
            assert(args.size == 1)
            IsNull(args.head.accept(this))

          case FunctionDefinitions.IS_NOT_NULL =>
            assert(args.size == 1)
            IsNotNull(args.head.accept(this))

          case FunctionDefinitions.IS_TRUE =>
            assert(args.size == 1)
            IsTrue(args.head.accept(this))

          case FunctionDefinitions.IS_FALSE =>
            assert(args.size == 1)
            IsFalse(args.head.accept(this))

          case FunctionDefinitions.IS_NOT_TRUE =>
            assert(args.size == 1)
            IsNotTrue(args.head.accept(this))

          case FunctionDefinitions.IS_NOT_FALSE =>
            assert(args.size == 1)
            IsNotFalse(args.head.accept(this))

          case FunctionDefinitions.IF =>
            assert(args.size == 3)
            If(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.BETWEEN =>
            assert(args.size == 3)
            Between(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.NOT_BETWEEN =>
            assert(args.size == 3)
            NotBetween(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.AVG =>
            assert(args.size == 1)
            Avg(args.head.accept(this))

          case FunctionDefinitions.COUNT =>
            assert(args.size == 1)
            Count(args.head.accept(this))

          case FunctionDefinitions.MAX =>
            assert(args.size == 1)
            Max(args.head.accept(this))

          case FunctionDefinitions.MIN =>
            assert(args.size == 1)
            Min(args.head.accept(this))

          case FunctionDefinitions.SUM =>
            assert(args.size == 1)
            Sum(args.head.accept(this))

          case FunctionDefinitions.SUM0 =>
            assert(args.size == 1)
            Sum0(args.head.accept(this))

          case FunctionDefinitions.STDDEV_POP =>
            assert(args.size == 1)
            StddevPop(args.head.accept(this))

          case FunctionDefinitions.STDDEV_SAMP =>
            assert(args.size == 1)
            StddevSamp(args.head.accept(this))

          case FunctionDefinitions.VAR_POP =>
            assert(args.size == 1)
            VarPop(args.head.accept(this))

          case FunctionDefinitions.VAR_SAMP =>
            assert(args.size == 1)
            VarSamp(args.head.accept(this))

          case FunctionDefinitions.COLLECT =>
            assert(args.size == 1)
            Collect(args.head.accept(this))

          case FunctionDefinitions.CHAR_LENGTH =>
            assert(args.size == 1)
            CharLength(args.head.accept(this))

          case FunctionDefinitions.INIT_CAP =>
            assert(args.size == 1)
            InitCap(args.head.accept(this))

          case FunctionDefinitions.LIKE =>
            assert(args.size == 2)
            Like(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.LOWER =>
            assert(args.size == 1)
            Lower(args.head.accept(this))

          case FunctionDefinitions.SIMILAR =>
            assert(args.size == 2)
            Similar(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.SUBSTRING =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Substring(args.head.accept(this), args.last.accept(this))
            } else {
              Substring(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.REPLACE =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              new Replace(args.head.accept(this), args.last.accept(this))
            } else {
              Replace(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.TRIM =>
            assert(args.size == 3)
            Trim(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.UPPER =>
            assert(args.size == 1)
            Upper(args.head.accept(this))

          case FunctionDefinitions.POSITION =>
            assert(args.size == 2)
            Position(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.OVERLAY =>
            assert(args.size == 3 || args.size == 4)
            if (args.size == 3) {
              new Overlay(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            } else {
              Overlay(
                args.head.accept(this),
                args(1).accept(this),
                args(2).accept(this),
                args.last.accept(this))
            }

          case FunctionDefinitions.CONCAT =>
            Concat(args.map(_.accept(this)))

          case FunctionDefinitions.CONCAT_WS =>
            assert(args.nonEmpty)
            ConcatWs(args.head.accept(this), args.tail.map(_.accept(this)))

          case FunctionDefinitions.LPAD =>
            assert(args.size == 3)
            Lpad(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.RPAD =>
            assert(args.size == 3)
            Rpad(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.REGEXP_EXTRACT =>
            assert(args.size == 2 || args.size == 3)
            if (args.size == 2) {
              RegexpExtract(args.head.accept(this), args.last.accept(this))
            } else {
              RegexpExtract(args.head.accept(this), args(1).accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.FROM_BASE64 =>
            assert(args.size == 1)
            FromBase64(args.head.accept(this))

          case FunctionDefinitions.TO_BASE64 =>
            assert(args.size == 1)
            ToBase64(args.head.accept(this))

          case FunctionDefinitions.UUID =>
            assert(args.isEmpty)
            UUID()

          case FunctionDefinitions.LTRIM =>
            assert(args.size == 1)
            LTrim(args.head.accept(this))

          case FunctionDefinitions.RTRIM =>
            assert(args.size == 1)
            RTrim(args.head.accept(this))

          case FunctionDefinitions.REPEAT =>
            assert(args.size == 2)
            Repeat(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.REGEXP_REPLACE =>
            assert(args.size == 3)
            RegexpReplace(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.PLUS =>
            assert(args.size == 2)
            Plus(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.MINUS =>
            assert(args.size == 2)
            Minus(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.DIVIDE =>
            assert(args.size == 2)
            Div(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.TIMES =>
            assert(args.size == 2)
            Mul(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.ABS =>
            assert(args.size == 1)
            Abs(args.head.accept(this))

          case FunctionDefinitions.CEIL =>
            assert(args.size == 1)
            Ceil(args.head.accept(this))

          case FunctionDefinitions.EXP =>
            assert(args.size == 1)
            Exp(args.head.accept(this))

          case FunctionDefinitions.FLOOR =>
            assert(args.size == 1)
            Floor(args.head.accept(this))

          case FunctionDefinitions.LOG10 =>
            assert(args.size == 1)
            Log10(args.head.accept(this))

          case FunctionDefinitions.LOG2 =>
            assert(args.size == 1)
            Log2(args.head.accept(this))

          case FunctionDefinitions.LN =>
            assert(args.size == 1)
            Ln(args.head.accept(this))

          case FunctionDefinitions.LOG =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              Log(args.head.accept(this))
            } else {
              Log(args.head.accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.POWER =>
            assert(args.size == 2)
            Power(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.MOD =>
            assert(args.size == 2)
            Mod(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.SQRT =>
            assert(args.size == 1)
            Sqrt(args.head.accept(this))

          case FunctionDefinitions.MINUS_PREFIX =>
            assert(args.size == 1)
            UnaryMinus(args.head.accept(this))

          case FunctionDefinitions.SIN =>
            assert(args.size == 1)
            Sin(args.head.accept(this))

          case FunctionDefinitions.COS =>
            assert(args.size == 1)
            Cos(args.head.accept(this))

          case FunctionDefinitions.SINH =>
            assert(args.size == 1)
            Sinh(args.head.accept(this))

          case FunctionDefinitions.TAN =>
            assert(args.size == 1)
            Tan(args.head.accept(this))

          case FunctionDefinitions.TANH =>
            assert(args.size == 1)
            Tanh(args.head.accept(this))

          case FunctionDefinitions.COT =>
            assert(args.size == 1)
            Cot(args.head.accept(this))

          case FunctionDefinitions.ASIN =>
            assert(args.size == 1)
            Asin(args.head.accept(this))

          case FunctionDefinitions.ACOS =>
            assert(args.size == 1)
            Acos(args.head.accept(this))

          case FunctionDefinitions.ATAN =>
            assert(args.size == 1)
            Atan(args.head.accept(this))

          case FunctionDefinitions.ATAN2 =>
            assert(args.size == 2)
            Atan2(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.COSH =>
            assert(args.size == 1)
            Cosh(args.head.accept(this))

          case FunctionDefinitions.DEGREES =>
            assert(args.size == 1)
            Degrees(args.head.accept(this))

          case FunctionDefinitions.RADIANS =>
            assert(args.size == 1)
            Radians(args.head.accept(this))

          case FunctionDefinitions.SIGN =>
            assert(args.size == 1)
            Sign(args.head.accept(this))

          case FunctionDefinitions.ROUND =>
            assert(args.size == 2)
            Round(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.PI =>
            assert(args.isEmpty)
            Pi()

          case FunctionDefinitions.E =>
            assert(args.isEmpty)
            E()

          case FunctionDefinitions.RAND =>
            assert(args.isEmpty || args.size == 1)
            if (args.isEmpty) {
              new Rand()
            } else {
              Rand(args.head.accept(this))
            }

          case FunctionDefinitions.RAND_INTEGER =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new RandInteger(args.head.accept(this))
            } else {
              RandInteger(args.head.accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.BIN =>
            assert(args.size == 1)
            Bin(args.head.accept(this))

          case FunctionDefinitions.HEX =>
            assert(args.size == 1)
            Hex(args.head.accept(this))

          case FunctionDefinitions.TRUNCATE =>
            assert(args.size == 1 || args.size == 2)
            if (args.size == 1) {
              new Truncate(args.head.accept(this))
            } else {
              Truncate(args.head.accept(this), args.last.accept(this))
            }

          case FunctionDefinitions.EXTRACT =>
            assert(args.size == 2)
            Extract(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.CURRENT_DATE =>
            assert(args.isEmpty)
            CurrentDate()

          case FunctionDefinitions.CURRENT_TIME =>
            assert(args.isEmpty)
            CurrentTime()

          case FunctionDefinitions.CURRENT_TIMESTAMP =>
            assert(args.isEmpty)
            CurrentTimestamp()

          case FunctionDefinitions.LOCAL_TIME =>
            assert(args.isEmpty)
            LocalTime()

          case FunctionDefinitions.LOCAL_TIMESTAMP =>
            assert(args.isEmpty)
            LocalTimestamp()

          case FunctionDefinitions.QUARTER =>
            assert(args.size == 1)
            Quarter(args.head.accept(this))

          case FunctionDefinitions.TEMPORAL_OVERLAPS =>
            assert(args.size == 4)
            TemporalOverlaps(
              args.head.accept(this),
              args(1).accept(this),
              args(2).accept(this),
              args.last.accept(this))

          case FunctionDefinitions.DATE_TIME_PLUS =>
            assert(args.size == 2)
            Plus(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.DATE_FORMAT =>
            assert(args.size == 2)
            DateFormat(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.TIMESTAMP_DIFF =>
            assert(args.size == 3)
            TimestampDiff(args.head.accept(this), args(1).accept(this), args.last.accept(this))

          case FunctionDefinitions.TEMPORAL_FLOOR =>
            assert(args.size == 2)
            TemporalFloor(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.TEMPORAL_CEIL =>
            assert(args.size == 2)
            TemporalCeil(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.AT =>
            assert(args.size == 2)
            ItemAt(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.CARDINALITY =>
            assert(args.size == 1)
            Cardinality(args.head.accept(this))

          case FunctionDefinitions.ARRAY =>
            ArrayConstructor(args.map(_.accept(this)))

          case FunctionDefinitions.ARRAY_ELEMENT =>
            assert(args.size == 1)
            ArrayElement(args.head.accept(this))

          case FunctionDefinitions.MAP =>
            MapConstructor(args.map(_.accept(this)))

          case FunctionDefinitions.ROW =>
            RowConstructor(args.map(_.accept(this)))

          case FunctionDefinitions.WIN_START =>
            assert(args.size == 1)
            WindowStart(args.head.accept(this))

          case FunctionDefinitions.WIN_END =>
            assert(args.size == 1)
            WindowEnd(args.head.accept(this))

          case FunctionDefinitions.ASC =>
            assert(args.size == 1)
            Asc(args.head.accept(this))

          case FunctionDefinitions.DESC =>
            assert(args.size == 1)
            Desc(args.head.accept(this))

          case FunctionDefinitions.MD5 =>
            assert(args.size == 1)
            Md5(args.head.accept(this))

          case FunctionDefinitions.SHA1 =>
            assert(args.size == 1)
            Sha1(args.head.accept(this))

          case FunctionDefinitions.SHA224 =>
            assert(args.size == 1)
            Sha224(args.head.accept(this))

          case FunctionDefinitions.SHA256 =>
            assert(args.size == 1)
            Sha256(args.head.accept(this))

          case FunctionDefinitions.SHA384 =>
            assert(args.size == 1)
            Sha384(args.head.accept(this))

          case FunctionDefinitions.SHA512 =>
            assert(args.size == 1)
            Sha512(args.head.accept(this))

          case FunctionDefinitions.SHA2 =>
            assert(args.size == 2)
            Sha2(args.head.accept(this), args.last.accept(this))

          case FunctionDefinitions.PROC_TIME =>
            ProctimeAttribute(args.head.accept(this))

          case FunctionDefinitions.ROW_TIME =>
            RowtimeAttribute(args.head.accept(this))

          case FunctionDefinitions.OVER_CALL =>
            UnresolvedOverCall(
              args.head.accept(this),
              args.last.accept(this)
            )

          case FunctionDefinitions.UNBOUNDED_RANGE =>
            UnboundedRange()

          case FunctionDefinitions.UNBOUNDED_ROW =>
            UnboundedRow()

          case FunctionDefinitions.CURRENT_RANGE =>
            CurrentRange()

          case FunctionDefinitions.CURRENT_ROW =>
            CurrentRow()

          case _ => PlannerCall(e.getName, args.map(_.accept(this)))
        }
    }
  }

  override def visitDistinctAgg(distinctAgg: DistinctAgg): PlannerExpression = {
    PlannerDistinctAgg(distinctAgg.getChildren.get(0).accept(this))
  }

  // needn't process TypeLiteral directly. It is processed with Cast
  override def visitTypeLiteral(typeLiteral: TypeLiteral): PlannerExpression = ???

  override def visitSymbolExpression(symbolExpression: SymbolExpression): PlannerExpression = {
    val tableSymbol = symbolExpression.getSymbol match {
      case TimeIntervalUnit.YEAR => PlannerTimeIntervalUnit.YEAR
      case TimeIntervalUnit.YEAR_TO_MONTH => PlannerTimeIntervalUnit.YEAR_TO_MONTH
      case TimeIntervalUnit.QUARTER => PlannerTimeIntervalUnit.QUARTER
      case TimeIntervalUnit.MONTH => PlannerTimeIntervalUnit.MONTH
      case TimeIntervalUnit.WEEK => PlannerTimeIntervalUnit.WEEK
      case TimeIntervalUnit.DAY => PlannerTimeIntervalUnit.DAY
      case TimeIntervalUnit.DAY_TO_HOUR => PlannerTimeIntervalUnit.DAY_TO_HOUR
      case TimeIntervalUnit.DAY_TO_MINUTE => PlannerTimeIntervalUnit.DAY_TO_MINUTE
      case TimeIntervalUnit.DAY_TO_SECOND => PlannerTimeIntervalUnit.DAY_TO_SECOND
      case TimeIntervalUnit.HOUR => PlannerTimeIntervalUnit.HOUR
      case TimeIntervalUnit.HOUR_TO_MINUTE => PlannerTimeIntervalUnit.HOUR_TO_MINUTE
      case TimeIntervalUnit.HOUR_TO_SECOND => PlannerTimeIntervalUnit.HOUR_TO_SECOND
      case TimeIntervalUnit.MINUTE => PlannerTimeIntervalUnit.MINUTE
      case TimeIntervalUnit.MINUTE_TO_SECOND => PlannerTimeIntervalUnit.MINUTE_TO_SECOND
      case TimeIntervalUnit.SECOND => PlannerTimeIntervalUnit.SECOND

      case TimePointUnit.YEAR => PlannerTimePointUnit.YEAR
      case TimePointUnit.MONTH => PlannerTimePointUnit.MONTH
      case TimePointUnit.DAY => PlannerTimePointUnit.DAY
      case TimePointUnit.HOUR => PlannerTimePointUnit.HOUR
      case TimePointUnit.MINUTE => PlannerTimePointUnit.MINUTE
      case TimePointUnit.SECOND => PlannerTimePointUnit.SECOND
      case TimePointUnit.QUARTER => PlannerTimePointUnit.QUARTER
      case TimePointUnit.WEEK => PlannerTimePointUnit.WEEK
      case TimePointUnit.MILLISECOND => PlannerTimePointUnit.MILLISECOND
      case TimePointUnit.MICROSECOND => PlannerTimePointUnit.MICROSECOND

      case TrimMode.BOTH => PlannerTrimMode.BOTH
      case TrimMode.LEADING => PlannerTrimMode.LEADING
      case TrimMode.TRAILING => PlannerTrimMode.TRAILING

      case _ =>
        throw new TableException("Unsupported TableSymbolValue: " + symbolExpression.getSymbol)
    }
    SymbolPlannerExpression(tableSymbol)
  }

  override def visitLiteral(literal: Literal): PlannerExpression = {
    if (!literal.getType.isPresent) {
      PlannerLiteral(literal.getValue)
    } else if (literal.getValue == null) {
      Null(literal.getType.get())
    } else {
      PlannerLiteral(literal.getValue, literal.getType.get())
    }
  }

  override def visitOther(other: Expression): PlannerExpression = {
    other match {
      case e: TableReference => PlannerTableReference(
        e.asInstanceOf[TableReference].getName,
        e.asInstanceOf[TableReference].getTable)
      case _ =>
        throw new TableException("Unrecognized expression [" + other + "]")
    }
  }

  override def visitFieldReference(fieldReference: FieldReference): PlannerExpression = {
    if (fieldReference.getResultType.isPresent) {
      ResolvedFieldReference(
        fieldReference.getName,
        fieldReference.getResultType.get())
    } else {
      UnresolvedFieldReference(fieldReference.getName)
    }
  }
}

object DefaultExpressionVisitor {
  val INSTANCE: DefaultExpressionVisitor = new DefaultExpressionVisitor
}
