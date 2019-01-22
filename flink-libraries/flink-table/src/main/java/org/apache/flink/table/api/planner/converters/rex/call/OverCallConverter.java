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

package org.apache.flink.table.api.planner.converters.rex.call;

import org.apache.flink.table.api.CurrentRange;
import org.apache.flink.table.api.CurrentRow;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.UnboundedRange;
import org.apache.flink.table.api.UnboundedRow;
import org.apache.flink.table.api.planner.visitor.AggregationSqlFunVisitorImpl;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.OverCall;
import org.apache.flink.table.typeutils.RowIntervalTypeInfo;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OrdinalReturnTypeInference;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

/**
 * OverCallConverter.
 */
public class OverCallConverter {
	public static RexNode toRexNode(OverCall overCall, ExpressionVisitorImpl visitor) {

		RexBuilder rexBuilder = visitor.getRelBuilder().getRexBuilder();

		// assemble aggregation
		SqlAggFunction operator =
			AggregationSqlFunVisitorImpl.getSqlAggFunction(
				(Aggregation) overCall.agg(), visitor.getRelBuilder());
		RelDataType aggResultType = ((FlinkTypeFactory) visitor.getRelBuilder()
			.getTypeFactory())
			.createTypeFromTypeInfo(overCall.agg().resultType(), true);

		// assemble exprs by agg children
		RexNode[] aggExprs = visitor.toRexNode(overCall.agg().children());

		// assemble order by key
		RexFieldCollation orderKey =
			new RexFieldCollation(visitor.toRexNode(overCall.orderBy()), new HashSet<>());
		ImmutableList<RexFieldCollation> orderKeys = ImmutableList.of(orderKey);

		// assemble partition by keys
		RexNode[] partitionKeys = visitor.toRexNode(overCall.partitionBy());

		// assemble bounds
		boolean isPhysical = overCall.preceding().resultType() instanceof RowIntervalTypeInfo;

		RexWindowBound lowerBound =
			createBound(visitor.getRelBuilder(), overCall.preceding(), SqlKind.PRECEDING);
		RexWindowBound upperBound =
			createBound(visitor.getRelBuilder(), overCall.following(), SqlKind.FOLLOWING);

		// build RexOver
		return rexBuilder.makeOver(
			aggResultType,
			operator,
			Arrays.asList(aggExprs),
			Arrays.asList(partitionKeys),
			orderKeys,
			lowerBound,
			upperBound,
			isPhysical,
			true,
			false,
			false);
	}

	public static RexWindowBound createBound(RelBuilder relBuilder, Expression bound, SqlKind sqlKind) {

		if (bound instanceof UnboundedRow || bound instanceof UnboundedRange) {

			SqlNode unbounded = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
			return RexWindowBound.create(unbounded, null);
		} else if (bound instanceof CurrentRow || bound instanceof CurrentRange) {
			SqlNode currentRow = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
			return RexWindowBound.create(currentRow, null);
		} else if (bound instanceof Literal) {
			RelDataType returnType = ((FlinkTypeFactory) relBuilder
				.getTypeFactory())
				.createTypeFromTypeInfo(Types.DECIMAL(), true);

			SqlPostfixOperator sqlOperator = new SqlPostfixOperator(
				sqlKind.name(),
				sqlKind,
				2,
				new OrdinalReturnTypeInference(0),
				null,
				null);

			SqlNode[] operands = new SqlNode[1];
			operands[0] = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

			SqlBasicCall node = new SqlBasicCall(sqlOperator, operands, SqlParserPos.ZERO);

			ArrayList<RexNode> expressions = new ArrayList<>();
			expressions.add(relBuilder.literal(((Literal) bound).value()));

			RexNode rexNode =
				relBuilder.getRexBuilder().makeCall(returnType, sqlOperator, expressions);

			return RexWindowBound.create(node, rexNode);
		} else {
			throw new RuntimeException("Unsupported window bound type: " +
				bound.getClass().getCanonicalName());
		}
	}
}
