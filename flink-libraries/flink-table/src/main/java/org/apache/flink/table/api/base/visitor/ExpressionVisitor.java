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

package org.apache.flink.table.api.base.visitor;

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
import org.apache.flink.table.plan.logical.Join;

/**
 * The RexNode visitor, define a visit method for each Expression.
 *
 * @param <T> In the implementation class is a RexNode type.
 */
public interface ExpressionVisitor<T> {

	//aggregations
	T visit(AggFunctionCall aggFunctionCall);


	//arithmetic
	T visit(Plus plus);

	T visit(UnaryMinus unaryMinus);

	T visit(Minus minus);

	T visit(Div div);

	T visit(Mul mul);

	T visit(Mod mod);


	//call
	T visit(OverCall overCall);

	T visit(ScalarFunctionCall scalarFunctionCall);


	//cast
	T visit(Cast cast);


	//collections
	T visit(RowConstructor rowConstructor);

	T visit(ArrayConstructor arrayConstructor);

	T visit(MapConstructor mapConstructor);

	T visit(ArrayElement arrayElement);

	T visit(Cardinality cardinality);

	T visit(ItemAt itemAt);


	//comparison
	T visit(EqualTo equalTo);

	T visit(NotEqualTo notEqualTo);

	T visit(GreaterThan greaterThan);

	T visit(GreaterThanOrEqual greaterThanOrEqual);

	T visit(LessThan lessThan);

	T visit(LessThanOrEqual lessThanOrEqual);

	T visit(IsNull isNull);

	T visit(IsNotNull isNotNull);

	T visit(IsTrue isTrue);

	T visit(IsFalse isFalse);

	T visit(IsNotTrue isNotTrue);

	T visit(IsNotFalse isNotFalse);

	T visit(Between between);

	T visit(NotBetween notBetween);


	//composite
	T visit(GetCompositeField getCompositeField);


	//field
	T visit(ResolvedFieldReference resolvedFieldReference);

	T visit(Alias alias);

	T visit(StreamRecordTimestamp streamRecordTimestamp);


	//hash
	T visit(Md5 md5);

	T visit(Sha1 sha1);

	T visit(Sha224 sha224);

	T visit(Sha256 sha256);

	T visit(Sha384 sha384);

	T visit(Sha512 sha512);

	T visit(Sha2 sha2);


	//literals
	T visit(Literal literal);

	T visit(Null nullExpr);


	//logic
	T visit(Not not);

	T visit(And and);

	T visit(Or or);

	T visit(If ifExpr);


	//math
	T visit(Abs abs);

	T visit(Ceil ceil);

	T visit(Exp exp);

	T visit(Floor floor);

	T visit(Log10 log10);

	T visit(Log2 log2);

	T visit(Cosh cosh);

	T visit(Log log);

	T visit(Ln ln);

	T visit(Power power);

	T visit(Sinh sinh);

	T visit(Sqrt sqrt);

	T visit(Sin sin);

	T visit(Cos cos);

	T visit(Tan tan);

	T visit(Tanh tanh);

	T visit(Cot cot);

	T visit(Asin asin);

	T visit(Acos acos);

	T visit(Atan atan);

	T visit(Atan2 atan2);

	T visit(Degrees degrees);

	T visit(Radians radians);

	T visit(Sign sign);

	T visit(Round round);

	T visit(Pi pi);

	T visit(E e);

	T visit(Rand rand);

	T visit(RandInteger randInteger);

	T visit(Bin bin);

	T visit(Hex hex);

	T visit(UUID uuid);


	//ordering
	T visit(Asc asc);

	T visit(Desc desc);

	//stringExpressions
	T visit(CharLength charLength);

	T visit(InitCap initCap);

	T visit(Like like);

	T visit(Lower lower);

	T visit(Similar similar);

	T visit(Substring substring);

	T visit(Trim trim);

	T visit(Upper upper);

	T visit(Position position);

	T visit(Overlay overlay);

	T visit(Concat concat);

	T visit(ConcatWs concatWs);

	T visit(Lpad lpad);

	T visit(Rpad rpad);

	T visit(RegexpReplace regexpReplace);

	T visit(RegexpExtract regexpExtract);

	T visit(FromBase64 fromBase64);

	T visit(ToBase64 toBase64);

	T visit(LTrim lTrim);

	T visit(RTrim rTrim);

	T visit(Repeat repeat);

	T visit(Replace replace);


	//subquery
	T visit(In in);


	//symbols
	T visit(SymbolExpression symbolExpression);


	//time
	T visit(Extract extract);

	T visit(TemporalFloor temporalFloor);

	T visit(TemporalCeil temporalCeil);

	T visit(CurrentDate currentDate);

	T visit(CurrentTime currentTime);

	T visit(CurrentTimestamp currentTimestamp);

	T visit(LocalTime localTime);

	T visit(LocalTimestamp localTimestamp);

	T visit(Quarter quarter);

	T visit(TemporalOverlaps temporalOverlaps);

	T visit(DateFormat dateFormat);

	T visit(TimestampDiff timestampDiff);

	//others
	T visit(Join.JoinFieldReference joinFieldReference);
}
