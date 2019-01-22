/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.planner.visitor;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.api.base.visitor.ExpressionVisitor;
import org.apache.flink.table.api.planner.converters.rex.CastRexConverter;
import org.apache.flink.table.api.planner.converters.rex.aggregations.AggFunctionCallConverter;
import org.apache.flink.table.api.planner.converters.rex.arithmetic.PlusConverter;
import org.apache.flink.table.api.planner.converters.rex.call.OverCallConverter;
import org.apache.flink.table.api.planner.converters.rex.call.ScalarFunctionCallConverter;
import org.apache.flink.table.api.planner.converters.rex.collection.ArrayConstructorConverter;
import org.apache.flink.table.api.planner.converters.rex.collection.MapConstructorConverter;
import org.apache.flink.table.api.planner.converters.rex.collection.RowConstructorConverter;
import org.apache.flink.table.api.planner.converters.rex.comparison.BetweenConverter;
import org.apache.flink.table.api.planner.converters.rex.comparison.NotBetweenConverter;
import org.apache.flink.table.api.planner.converters.rex.literals.LiteralConverter;
import org.apache.flink.table.api.planner.converters.rex.literals.NullConverter;
import org.apache.flink.table.api.planner.converters.rex.subquery.InConverter;
import org.apache.flink.table.api.planner.converters.rex.time.QuarterConverter;
import org.apache.flink.table.api.planner.converters.rex.time.TemporalOverlapsConverter;
import org.apache.flink.table.expressions.Abs;
import org.apache.flink.table.expressions.Acos;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Alias;
import org.apache.flink.table.expressions.And;
import org.apache.flink.table.expressions.ArrayConstructor;
import org.apache.flink.table.expressions.ArrayElement;
import org.apache.flink.table.expressions.Asc;
import org.apache.flink.table.expressions.Asin;
import org.apache.flink.table.expressions.Atan;
import org.apache.flink.table.expressions.Atan2;
import org.apache.flink.table.expressions.Between;
import org.apache.flink.table.expressions.Bin;
import org.apache.flink.table.expressions.Cardinality;
import org.apache.flink.table.expressions.Cast;
import org.apache.flink.table.expressions.Ceil;
import org.apache.flink.table.expressions.CharLength;
import org.apache.flink.table.expressions.Concat;
import org.apache.flink.table.expressions.ConcatWs;
import org.apache.flink.table.expressions.Cos;
import org.apache.flink.table.expressions.Cosh;
import org.apache.flink.table.expressions.Cot;
import org.apache.flink.table.expressions.CurrentDate;
import org.apache.flink.table.expressions.CurrentTime;
import org.apache.flink.table.expressions.CurrentTimestamp;
import org.apache.flink.table.expressions.DateFormat;
import org.apache.flink.table.expressions.Degrees;
import org.apache.flink.table.expressions.Desc;
import org.apache.flink.table.expressions.Div;
import org.apache.flink.table.expressions.E;
import org.apache.flink.table.expressions.EqualTo;
import org.apache.flink.table.expressions.Exp;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Extract;
import org.apache.flink.table.expressions.Floor;
import org.apache.flink.table.expressions.FromBase64;
import org.apache.flink.table.expressions.GetCompositeField;
import org.apache.flink.table.expressions.GreaterThan;
import org.apache.flink.table.expressions.GreaterThanOrEqual;
import org.apache.flink.table.expressions.Hex;
import org.apache.flink.table.expressions.If;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.expressions.InitCap;
import org.apache.flink.table.expressions.IsFalse;
import org.apache.flink.table.expressions.IsNotFalse;
import org.apache.flink.table.expressions.IsNotNull;
import org.apache.flink.table.expressions.IsNotTrue;
import org.apache.flink.table.expressions.IsNull;
import org.apache.flink.table.expressions.IsTrue;
import org.apache.flink.table.expressions.ItemAt;
import org.apache.flink.table.expressions.LTrim;
import org.apache.flink.table.expressions.LessThan;
import org.apache.flink.table.expressions.LessThanOrEqual;
import org.apache.flink.table.expressions.Like;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.Ln;
import org.apache.flink.table.expressions.LocalTime;
import org.apache.flink.table.expressions.LocalTimestamp;
import org.apache.flink.table.expressions.Log;
import org.apache.flink.table.expressions.Log10;
import org.apache.flink.table.expressions.Log2;
import org.apache.flink.table.expressions.Lower;
import org.apache.flink.table.expressions.Lpad;
import org.apache.flink.table.expressions.MapConstructor;
import org.apache.flink.table.expressions.Md5;
import org.apache.flink.table.expressions.Minus;
import org.apache.flink.table.expressions.Mod;
import org.apache.flink.table.expressions.Mul;
import org.apache.flink.table.expressions.Not;
import org.apache.flink.table.expressions.NotBetween;
import org.apache.flink.table.expressions.NotEqualTo;
import org.apache.flink.table.expressions.Null;
import org.apache.flink.table.expressions.Or;
import org.apache.flink.table.expressions.OverCall;
import org.apache.flink.table.expressions.Overlay;
import org.apache.flink.table.expressions.Pi;
import org.apache.flink.table.expressions.Plus;
import org.apache.flink.table.expressions.Position;
import org.apache.flink.table.expressions.Power;
import org.apache.flink.table.expressions.Quarter;
import org.apache.flink.table.expressions.RTrim;
import org.apache.flink.table.expressions.Radians;
import org.apache.flink.table.expressions.Rand;
import org.apache.flink.table.expressions.RandInteger;
import org.apache.flink.table.expressions.RegexpExtract;
import org.apache.flink.table.expressions.RegexpReplace;
import org.apache.flink.table.expressions.Repeat;
import org.apache.flink.table.expressions.Replace;
import org.apache.flink.table.expressions.ResolvedFieldReference;
import org.apache.flink.table.expressions.Round;
import org.apache.flink.table.expressions.RowConstructor;
import org.apache.flink.table.expressions.Rpad;
import org.apache.flink.table.expressions.ScalarFunctionCall;
import org.apache.flink.table.expressions.Sha1;
import org.apache.flink.table.expressions.Sha2;
import org.apache.flink.table.expressions.Sha224;
import org.apache.flink.table.expressions.Sha256;
import org.apache.flink.table.expressions.Sha384;
import org.apache.flink.table.expressions.Sha512;
import org.apache.flink.table.expressions.Sign;
import org.apache.flink.table.expressions.Similar;
import org.apache.flink.table.expressions.Sin;
import org.apache.flink.table.expressions.Sinh;
import org.apache.flink.table.expressions.Sqrt;
import org.apache.flink.table.expressions.StreamRecordTimestamp;
import org.apache.flink.table.expressions.Substring;
import org.apache.flink.table.expressions.SymbolExpression;
import org.apache.flink.table.expressions.Tan;
import org.apache.flink.table.expressions.Tanh;
import org.apache.flink.table.expressions.TemporalCeil;
import org.apache.flink.table.expressions.TemporalFloor;
import org.apache.flink.table.expressions.TemporalOverlaps;
import org.apache.flink.table.expressions.TimestampDiff;
import org.apache.flink.table.expressions.ToBase64;
import org.apache.flink.table.expressions.Trim;
import org.apache.flink.table.expressions.UUID;
import org.apache.flink.table.expressions.UnaryMinus;
import org.apache.flink.table.expressions.Upper;
import org.apache.flink.table.functions.sql.ScalarSqlFunctions;
import org.apache.flink.table.functions.sql.ScalarSqlFunctions$;
import org.apache.flink.table.functions.sql.StreamRecordTimestampSqlFunction$;
import org.apache.flink.table.plan.logical.Join;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;

import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * RexNodeVisitorImpl.
 */
public class ExpressionVisitorImpl implements ExpressionVisitor<RexNode> {
	protected RelBuilder relBuilder;

	public ExpressionVisitorImpl(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	public RelBuilder getRelBuilder() {
		return relBuilder;
	}

	public static RexNode toRexNode(Expression expr, RelBuilder relBuilder) {
		return new ExpressionVisitorImpl(relBuilder).toRexNode(expr);
	}

	public RexNode toRexNode(Expression expr) {
		return expr.accept(this);
	}

	public RexNode[] toRexNode(Seq<? extends Expression> expressions) {
		return JavaConversions.seqAsJavaList(expressions)
			.stream().map(this::toRexNode).toArray(RexNode[]::new);
	}

	public RexNode[] toRexNode(Expression... expressions) {
		return Arrays.stream(expressions).map(this::toRexNode).toArray(RexNode[]::new);
	}

	public RexNode toRexNode(SqlOperator operator, Expression... operand) {
		return relBuilder.call(operator, toRexNode(operand));
	}

	public RexNode toRexNode(SqlOperator operator, Seq<? extends Expression> operands) {
		return relBuilder.call(operator, toRexNode(operands));
	}

	@Override
	public RexNode visit(AggFunctionCall aggFunctionCall) {
		return AggFunctionCallConverter.toRexNode(aggFunctionCall, this);
	}

	@Override
	public RexNode visit(Plus plus) {
		return PlusConverter.toRexNode(plus, this);
	}

	@Override
	public RexNode visit(UnaryMinus unaryMinus) {
		return toRexNode(SqlStdOperatorTable.UNARY_MINUS, unaryMinus.children());
	}

	@Override
	public RexNode visit(Minus minus) {
		return toRexNode(SqlStdOperatorTable.MINUS, minus.children());
	}

	@Override
	public RexNode visit(Div div) {
		return toRexNode(SqlStdOperatorTable.DIVIDE, div.children());
	}

	@Override
	public RexNode visit(Mul mul) {
		return toRexNode(SqlStdOperatorTable.MULTIPLY, mul.children());
	}

	@Override
	public RexNode visit(Mod mod) {
		return toRexNode(SqlStdOperatorTable.MOD, mod.children());
	}

	@Override
	public RexNode visit(OverCall overCall) {
		return OverCallConverter.toRexNode(overCall, this);
	}

	@Override
	public RexNode visit(ScalarFunctionCall scalarFunctionCall) {
		return ScalarFunctionCallConverter.toRexNode(scalarFunctionCall, this);
	}

	@Override
	public RexNode visit(Cast cast) {
		return CastRexConverter.toRexNode(cast, this);
	}

	@Override
	public RexNode visit(RowConstructor rowConstructor) {
		return RowConstructorConverter.toRexNode(rowConstructor, this);
	}

	@Override
	public RexNode visit(ArrayConstructor arrayConstructor) {
		return ArrayConstructorConverter.toRexNode(arrayConstructor, this);
	}

	@Override
	public RexNode visit(MapConstructor mapConstructor) {
		return MapConstructorConverter.toRexNode(mapConstructor, this);
	}

	@Override
	public RexNode visit(ArrayElement arrayElement) {
		return toRexNode(SqlStdOperatorTable.ELEMENT, arrayElement.children());
	}

	@Override
	public RexNode visit(Cardinality cardinality) {
		return toRexNode(SqlStdOperatorTable.CARDINALITY, cardinality.children());
	}

	@Override
	public RexNode visit(ItemAt itemAt) {
		return toRexNode(SqlStdOperatorTable.ITEM, itemAt.children());
	}

	@Override
	public RexNode visit(EqualTo equalTo) {
		return toRexNode(SqlStdOperatorTable.EQUALS, equalTo.children());
	}

	@Override
	public RexNode visit(NotEqualTo notEqualTo) {
		return toRexNode(SqlStdOperatorTable.NOT_EQUALS, notEqualTo.children());
	}

	@Override
	public RexNode visit(GreaterThan greaterThan) {
		return toRexNode(SqlStdOperatorTable.GREATER_THAN, greaterThan.children());
	}

	@Override
	public RexNode visit(GreaterThanOrEqual greaterThanOrEqual) {
		return toRexNode(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, greaterThanOrEqual.children());
	}

	@Override
	public RexNode visit(LessThan lessThan) {
		return toRexNode(SqlStdOperatorTable.LESS_THAN, lessThan.children());
	}

	@Override
	public RexNode visit(LessThanOrEqual lessThanOrEqual) {
		return toRexNode(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, lessThanOrEqual.children());
	}

	@Override
	public RexNode visit(IsNull isNull) {
		return relBuilder.isNull(toRexNode(isNull.child()));
	}

	@Override
	public RexNode visit(IsNotNull isNotNull) {
		return relBuilder.isNotNull(toRexNode(isNotNull.child()));
	}

	@Override
	public RexNode visit(IsTrue isTrue) {
		return toRexNode(SqlStdOperatorTable.IS_TRUE, isTrue.children());
	}

	@Override
	public RexNode visit(IsFalse isFalse) {
		return toRexNode(SqlStdOperatorTable.IS_FALSE, isFalse.children());
	}

	@Override
	public RexNode visit(IsNotTrue isNotTrue) {
		return toRexNode(SqlStdOperatorTable.IS_NOT_TRUE, isNotTrue.children());
	}

	@Override
	public RexNode visit(IsNotFalse isNotFalse) {
		return toRexNode(SqlStdOperatorTable.IS_NOT_FALSE, isNotFalse.children());
	}

	@Override
	public RexNode visit(Between between) {
		return BetweenConverter.toRexNode(between, this);
	}

	@Override
	public RexNode visit(NotBetween notBetween) {
		return NotBetweenConverter.toRexNode(notBetween, this);
	}

	@Override
	public RexNode visit(GetCompositeField getCompositeField) {
		return relBuilder.getRexBuilder().makeFieldAccess(
			toRexNode(getCompositeField.child()), (Integer) getCompositeField.fieldIndex().get());
	}

	@Override
	public RexNode visit(ResolvedFieldReference resolvedFieldReference) {
		return relBuilder.field(resolvedFieldReference.name());
	}

	@Override
	public RexNode visit(Alias alias) {
		return relBuilder.alias(toRexNode(alias.child()), alias.name());
	}

	@Override
	public RexNode visit(StreamRecordTimestamp streamRecordTimestamp) {
		return relBuilder.getRexBuilder().makeCall(StreamRecordTimestampSqlFunction$.MODULE$);
	}

	@Override
	public RexNode visit(Md5 md5) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.MD5(), md5.children());
	}

	@Override
	public RexNode visit(Sha1 sha1) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA1(), sha1.children());
	}

	@Override
	public RexNode visit(Sha224 sha224) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA224(), sha224.children());
	}

	@Override
	public RexNode visit(Sha256 sha256) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA256(), sha256.children());
	}

	@Override
	public RexNode visit(Sha384 sha384) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA384(), sha384.children());
	}

	@Override
	public RexNode visit(Sha512 sha512) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA512(), sha512.children());
	}

	@Override
	public RexNode visit(Sha2 sha2) {
		return toRexNode(ScalarSqlFunctions$.MODULE$.SHA2(), sha2.children());
	}

	@Override
	public RexNode visit(Literal literal) {
		return LiteralConverter.toRexNode(literal, this);
	}

	@Override
	public RexNode visit(Null nullExpr) {
		return NullConverter.toRexNode(nullExpr, this);
	}

	@Override
	public RexNode visit(Not not) {
		return relBuilder.not(toRexNode(not.child()));
	}

	@Override
	public RexNode visit(And and) {
		return relBuilder.and(toRexNode(and.left()), toRexNode(and.right()));
	}

	@Override
	public RexNode visit(Or or) {
		return relBuilder.or(toRexNode(or.left()), toRexNode(or.right()));
	}

	@Override
	public RexNode visit(If ifExpr) {
		return toRexNode(SqlStdOperatorTable.CASE, ifExpr.children());
	}

	@Override
	public RexNode visit(Abs abs) {
		return toRexNode(SqlStdOperatorTable.ABS, abs.children());
	}

	@Override
	public RexNode visit(Ceil ceil) {
		return toRexNode(SqlStdOperatorTable.CEIL, ceil.children());
	}

	@Override
	public RexNode visit(Exp exp) {
		return toRexNode(SqlStdOperatorTable.EXP, exp.children());
	}

	@Override
	public RexNode visit(Floor floor) {
		return toRexNode(SqlStdOperatorTable.FLOOR, floor.children());
	}

	@Override
	public RexNode visit(Log10 log10) {
		return toRexNode(SqlStdOperatorTable.LOG10, log10.children());
	}

	@Override
	public RexNode visit(Log2 log2) {
		return toRexNode(ScalarSqlFunctions.LOG2(), log2.children());
	}

	@Override
	public RexNode visit(Cosh cosh) {
		return toRexNode(ScalarSqlFunctions.COSH(), cosh.children());
	}

	@Override
	public RexNode visit(Log log) {
		return toRexNode(ScalarSqlFunctions.LOG(), log.children());
	}

	@Override
	public RexNode visit(Ln ln) {
		return toRexNode(SqlStdOperatorTable.LN, ln.children());
	}

	@Override
	public RexNode visit(Power power) {
		return toRexNode(SqlStdOperatorTable.POWER, power.children());
	}

	@Override
	public RexNode visit(Sinh sinh) {
		return toRexNode(ScalarSqlFunctions.SINH(), sinh.children());
	}

	@Override
	public RexNode visit(Sqrt sqrt) {
		return toRexNode(SqlStdOperatorTable.POWER, sqrt.child(),
			Literal.apply(0.5, BasicTypeInfo.FLOAT_TYPE_INFO));
	}

	@Override
	public RexNode visit(Sin sin) {
		return toRexNode(SqlStdOperatorTable.SIN, sin.children());
	}

	@Override
	public RexNode visit(Cos cos) {
		return toRexNode(SqlStdOperatorTable.COS, cos.children());
	}

	@Override
	public RexNode visit(Tan tan) {
		return toRexNode(SqlStdOperatorTable.TAN, tan.children());
	}

	@Override
	public RexNode visit(Tanh tanh) {
		return toRexNode(ScalarSqlFunctions.TANH(), tanh.children());
	}

	@Override
	public RexNode visit(Cot cot) {
		return toRexNode(SqlStdOperatorTable.COT, cot.children());
	}

	@Override
	public RexNode visit(Asin asin) {
		return toRexNode(SqlStdOperatorTable.ASIN, asin.children());
	}

	@Override
	public RexNode visit(Acos acos) {
		return toRexNode(SqlStdOperatorTable.ACOS, acos.children());
	}

	@Override
	public RexNode visit(Atan atan) {
		return toRexNode(SqlStdOperatorTable.ATAN, atan.children());
	}

	@Override
	public RexNode visit(Atan2 atan2) {
		return toRexNode(SqlStdOperatorTable.ATAN2, atan2.children());
	}

	@Override
	public RexNode visit(Degrees degrees) {
		return toRexNode(SqlStdOperatorTable.DEGREES, degrees.children());
	}

	@Override
	public RexNode visit(Radians radians) {
		return toRexNode(SqlStdOperatorTable.RADIANS, radians.children());
	}

	@Override
	public RexNode visit(Sign sign) {
		return toRexNode(SqlStdOperatorTable.SIGN, sign.children());
	}

	@Override
	public RexNode visit(Round round) {
		return toRexNode(SqlStdOperatorTable.ROUND, round.children());
	}

	@Override
	public RexNode visit(Pi pi) {
		return relBuilder.call(SqlStdOperatorTable.PI);
	}

	@Override
	public RexNode visit(E e) {
		return relBuilder.call(ScalarSqlFunctions.E());
	}

	@Override
	public RexNode visit(Rand rand) {
		return toRexNode(SqlStdOperatorTable.RAND, rand.children());
	}

	@Override
	public RexNode visit(RandInteger randInteger) {
		return toRexNode(SqlStdOperatorTable.RAND_INTEGER, randInteger.children());
	}

	@Override
	public RexNode visit(Bin bin) {
		return toRexNode(ScalarSqlFunctions.BIN(), bin.children());
	}

	@Override
	public RexNode visit(Hex hex) {
		return toRexNode(ScalarSqlFunctions.HEX(), hex.children());
	}

	@Override
	public RexNode visit(UUID uuid) {
		return relBuilder.call(ScalarSqlFunctions.UUID());
	}

	@Override
	public RexNode visit(Asc asc) {
		return toRexNode(asc.child());
	}

	@Override
	public RexNode visit(Desc desc) {
		return relBuilder.desc(toRexNode(desc.child()));
	}

	@Override
	public RexNode visit(CharLength charLength) {
		return toRexNode(SqlStdOperatorTable.CHAR_LENGTH, charLength.children());
	}

	@Override
	public RexNode visit(InitCap initCap) {
		return toRexNode(SqlStdOperatorTable.INITCAP, initCap.children());
	}

	@Override
	public RexNode visit(Like like) {
		return toRexNode(SqlStdOperatorTable.LIKE, like.children());
	}

	@Override
	public RexNode visit(Lower lower) {
		return toRexNode(SqlStdOperatorTable.LOWER, lower.children());
	}

	@Override
	public RexNode visit(Similar similar) {
		return toRexNode(SqlStdOperatorTable.SIMILAR_TO, similar.children());
	}

	@Override
	public RexNode visit(Substring substring) {
		return toRexNode(SqlStdOperatorTable.SUBSTRING, substring.children());
	}

	@Override
	public RexNode visit(Trim trim) {
		return toRexNode(SqlStdOperatorTable.TRIM, trim.children());
	}

	@Override
	public RexNode visit(Upper upper) {
		return toRexNode(SqlStdOperatorTable.UPPER, upper.children());
	}

	@Override
	public RexNode visit(Position position) {
		return toRexNode(SqlStdOperatorTable.POSITION, position.children());
	}

	@Override
	public RexNode visit(Overlay overlay) {
		return toRexNode(SqlStdOperatorTable.OVERLAY, overlay.children());
	}

	@Override
	public RexNode visit(Concat concat) {
		return toRexNode(ScalarSqlFunctions.CONCAT(), concat.children());
	}

	@Override
	public RexNode visit(ConcatWs concatWs) {
		return toRexNode(ScalarSqlFunctions.CONCAT_WS(), concatWs.children());
	}

	@Override
	public RexNode visit(Lpad lpad) {
		return toRexNode(ScalarSqlFunctions.LPAD(), lpad.children());
	}

	@Override
	public RexNode visit(Rpad rpad) {
		return toRexNode(ScalarSqlFunctions.RPAD(), rpad.children());
	}

	@Override
	public RexNode visit(RegexpReplace regexpReplace) {
		return toRexNode(ScalarSqlFunctions.REGEXP_REPLACE(), regexpReplace.children());
	}

	@Override
	public RexNode visit(RegexpExtract regexpExtract) {
		return toRexNode(ScalarSqlFunctions.REGEXP_EXTRACT(), regexpExtract.children());
	}

	@Override
	public RexNode visit(FromBase64 fromBase64) {
		return toRexNode(ScalarSqlFunctions.FROM_BASE64(), fromBase64.children());
	}

	@Override
	public RexNode visit(ToBase64 toBase64) {
		return toRexNode(ScalarSqlFunctions.TO_BASE64(), toBase64.children());
	}

	@Override
	public RexNode visit(LTrim lTrim) {
		return toRexNode(ScalarSqlFunctions.LTRIM(), lTrim.children());
	}

	@Override
	public RexNode visit(RTrim rTrim) {
		return toRexNode(ScalarSqlFunctions.RTRIM(), rTrim.children());
	}

	@Override
	public RexNode visit(Repeat repeat) {
		return toRexNode(ScalarSqlFunctions.REPEAT(), repeat.children());
	}

	@Override
	public RexNode visit(Replace replace) {
		return toRexNode(SqlStdOperatorTable.REPLACE, replace.children());
	}

	@Override
	public RexNode visit(In in) {
		return InConverter.toRexNode(in, this);
	}

	@Override
	public RexNode visit(SymbolExpression symbolExpression) {
		return relBuilder.getRexBuilder().makeFlag(symbolExpression.symbol().javaEnum());
	}

	@Override
	public RexNode visit(Extract extract) {
		return toRexNode(SqlStdOperatorTable.EXTRACT, extract.children());
	}

	@Override
	public RexNode visit(TemporalFloor temporalFloor) {
		return relBuilder.call(SqlStdOperatorTable.FLOOR,
			toRexNode(temporalFloor.temporal()), toRexNode(temporalFloor.timeIntervalUnit()));
	}

	@Override
	public RexNode visit(TemporalCeil temporalCeil) {
		return relBuilder.call(SqlStdOperatorTable.CEIL,
			toRexNode(temporalCeil.temporal()), toRexNode(temporalCeil.timeIntervalUnit()));
	}

	@Override
	public RexNode visit(CurrentDate currentDate) {
		return relBuilder.call(SqlStdOperatorTable.CURRENT_DATE);
	}

	@Override
	public RexNode visit(CurrentTime currentTime) {
		return relBuilder.call(SqlStdOperatorTable.CURRENT_TIME);
	}

	@Override
	public RexNode visit(CurrentTimestamp currentTimestamp) {
		return relBuilder.call(SqlStdOperatorTable.CURRENT_TIMESTAMP);
	}

	@Override
	public RexNode visit(LocalTime localTime) {
		return relBuilder.call(SqlStdOperatorTable.LOCALTIME);
	}

	@Override
	public RexNode visit(LocalTimestamp localTimestamp) {
		return relBuilder.call(SqlStdOperatorTable.LOCALTIMESTAMP);
	}

	@Override
	public RexNode visit(Quarter quarter) {
		return QuarterConverter.toRexNode(quarter, this);
	}

	@Override
	public RexNode visit(TemporalOverlaps temporalOverlaps) {
		return TemporalOverlapsConverter.toRexNode(temporalOverlaps, this);
	}

	@Override
	public RexNode visit(DateFormat dateFormat) {
		return toRexNode(ScalarSqlFunctions.DATE_FORMAT(), dateFormat.children());
	}

	@Override
	public RexNode visit(TimestampDiff timestampDiff) {
		return toRexNode(SqlStdOperatorTable.TIMESTAMP_DIFF,
			timestampDiff.timePointUnit(), timestampDiff.timePoint2(), timestampDiff.timePoint1());
	}

	@Override
	public RexNode visit(Join.JoinFieldReference joinFieldReference) {
		return joinFieldReference.toRexNode(relBuilder);
	}
}
