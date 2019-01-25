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

package org.apache.flink.table.api.planner.converters.rex.comparison;

import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.expressions.Between;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * BetweenConverter.
 */
public class BetweenConverter {
	public static RexNode toRexNode(Between between, ExpressionVisitorImpl visitor) {
		return visitor.getRelBuilder().and(
			visitor.getRelBuilder().call(
				SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
				visitor.toRexNode(between.expr()),
				visitor.toRexNode(between.lowerBound())
			),
			visitor.getRelBuilder().call(
				SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
				visitor.toRexNode(between.expr()),
					visitor.toRexNode(between.upperBound())
			)
		);
	}
}
