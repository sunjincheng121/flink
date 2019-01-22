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
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.AggFunctionCall;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.Avg;
import org.apache.flink.table.expressions.Collect;
import org.apache.flink.table.expressions.Count;
import org.apache.flink.table.expressions.DistinctAgg;
import org.apache.flink.table.expressions.Max;
import org.apache.flink.table.expressions.Min;
import org.apache.flink.table.expressions.StddevPop;
import org.apache.flink.table.expressions.StddevSamp;
import org.apache.flink.table.expressions.Sum;
import org.apache.flink.table.expressions.Sum0;
import org.apache.flink.table.expressions.VarPop;
import org.apache.flink.table.expressions.VarSamp;
import org.apache.flink.table.functions.utils.AggSqlFunction;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.tools.RelBuilder;

/**
 * AggregationSqlFunVisitorImpl.
 */
public class AggregationSqlFunVisitorImpl implements AggregationVisitor<SqlAggFunction> {

	protected RelBuilder relBuilder;

	public RelBuilder getRelBuilder() {
		return relBuilder;
	}

	public AggregationSqlFunVisitorImpl(RelBuilder relBuilder) {
		this.relBuilder = relBuilder;
	}

	public static SqlAggFunction getSqlAggFunction(Aggregation agg, RelBuilder relBuilder) {
		AggregationSqlFunVisitorImpl visitor = new AggregationSqlFunVisitorImpl(relBuilder);
		return agg.accept(visitor);
	}

	@Override
	public SqlAggFunction visit(DistinctAgg distinctAgg) {
		return ((Aggregation) distinctAgg.child()).accept(this);
	}

	@Override
	public SqlAggFunction visit(Sum sum) {
		FlinkTypeFactory flinkTypeFactory =
			(FlinkTypeFactory) relBuilder.getTypeFactory();
		return new SqlSumAggFunction(
			flinkTypeFactory.createTypeFromTypeInfo(sum.resultType(), true));
	}

	@Override
	public SqlAggFunction visit(Sum0 sum) {
		return SqlStdOperatorTable.SUM0;
	}

	@Override
	public SqlAggFunction visit(Min min) {
		return SqlStdOperatorTable.MIN;
	}

	@Override
	public SqlAggFunction visit(Max max) {
		return SqlStdOperatorTable.MAX;
	}

	@Override
	public SqlAggFunction visit(Count count) {
		return SqlStdOperatorTable.COUNT;
	}

	@Override
	public SqlAggFunction visit(Avg avg) {
		return SqlStdOperatorTable.AVG;
	}

	@Override
	public SqlAggFunction visit(Collect collect) {
		return SqlStdOperatorTable.COLLECT;
	}

	@Override
	public SqlAggFunction visit(StddevPop stddevPop) {
		return SqlStdOperatorTable.STDDEV_POP;
	}

	@Override
	public SqlAggFunction visit(StddevSamp stddevSamp) {
		return SqlStdOperatorTable.STDDEV_SAMP;
	}

	@Override
	public SqlAggFunction visit(VarPop varPop) {
		return SqlStdOperatorTable.VAR_POP;
	}

	@Override
	public SqlAggFunction visit(VarSamp varSamp) {
		return SqlStdOperatorTable.VAR_SAMP;
	}

	@Override
	public SqlAggFunction visit(AggFunctionCall aggFunctionCall) {
		FlinkTypeFactory flinkTypeFactory =
			(FlinkTypeFactory) relBuilder.getTypeFactory();
		return new AggSqlFunction(
			aggFunctionCall.aggregateFunction().functionIdentifier(),
			aggFunctionCall.aggregateFunction().toString(),
			aggFunctionCall.aggregateFunction(),
			aggFunctionCall.resultType(),
			aggFunctionCall.accTypeInfo(),
			flinkTypeFactory,
			aggFunctionCall.aggregateFunction().requiresOver()
		);
	}
}
