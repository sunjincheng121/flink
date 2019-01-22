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

import org.apache.flink.table.api.base.visitor.AggregationVisitor;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.Avg;
import org.apache.flink.table.expressions.Collect;
import org.apache.flink.table.expressions.Count;
import org.apache.flink.table.expressions.DistinctAgg;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Max;
import org.apache.flink.table.expressions.Min;
import org.apache.flink.table.expressions.StddevPop;
import org.apache.flink.table.expressions.StddevSamp;
import org.apache.flink.table.expressions.Sum;
import org.apache.flink.table.expressions.Sum0;
import org.apache.flink.table.expressions.VarPop;
import org.apache.flink.table.expressions.VarSamp;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import scala.collection.JavaConversions;

/**
 * AggregationCallVisitorImpl.
 */
public class AggregationCallVisitorImpl implements AggregationVisitor<RelBuilder.AggCall>{

	protected RelBuilder relBuilder;

	public RelBuilder getRelBuilder() {
		return relBuilder;
	}

	protected boolean isDistinct = false;

	public void setDistinct(boolean distinct) {
		isDistinct = distinct;
	}

	public boolean getDistinct() {
		return isDistinct;
	}

	protected String name;

	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	private AggregationCallVisitorImpl(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	public static RelBuilder.AggCall toAggCall(
		Aggregation agg, String name, boolean isDistinct, RelBuilder relBuilder) {
		AggregationCallVisitorImpl visitor = new AggregationCallVisitorImpl(relBuilder);
		visitor.setDistinct(isDistinct);
		visitor.setName(name);
		return agg.accept(visitor);
	}

	@Override
	public RelBuilder.AggCall visit(DistinctAgg distinctAgg) {
		Aggregation child = (Aggregation) distinctAgg.child();
		setDistinct(true);
		return child.accept(this);
	}

	@Override
	public RelBuilder.AggCall visit(Sum sum) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.SUM,
			isDistinct,
			false,
			null,
			name,
			sum.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Sum0 sum) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.SUM0,
			isDistinct,
			false,
			null,
			name,
			sum.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Min min) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.MIN,
			isDistinct,
			false,
			null,
			name,
			min.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Max max) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.MAX,
			isDistinct,
			false,
			null,
			name,
			max.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Count count) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.COUNT,
			isDistinct,
			false,
			null,
			name,
			count.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Avg avg) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.AVG,
			isDistinct,
			false,
			null,
			name,
			avg.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(Collect collect) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.COLLECT,
			isDistinct,
			false,
			null,
			name,
			collect.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(StddevPop stddevPop) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.STDDEV_POP,
			isDistinct,
			false,
			null,
			name,
			stddevPop.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(StddevSamp stddevSamp) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.STDDEV_SAMP,
			isDistinct,
			false,
			null,
			name,
			stddevSamp.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(VarPop varPop) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.VAR_POP,
			isDistinct,
			false,
			null,
			name,
			varPop.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(VarSamp varSamp) {
		return relBuilder.aggregateCall(
			SqlStdOperatorTable.VAR_SAMP,
			isDistinct,
			false,
			null,
			name,
			varSamp.child().accept(new ExpressionVisitorImpl(relBuilder)));
	}

	@Override
	public RelBuilder.AggCall visit(AggFunctionCall aggFunctionCall) {
		RexNode[] args = JavaConversions.seqAsJavaList(aggFunctionCall.args()).stream()
			.map((Expression e) -> e.accept(new ExpressionVisitorImpl(relBuilder)))
			.toArray(RexNode[]::new);
		return relBuilder.aggregateCall(
			AggregationSqlFunVisitorImpl.getSqlAggFunction(aggFunctionCall, relBuilder),
			isDistinct,
			false,
			null,
			name,
			args);
	}
}
