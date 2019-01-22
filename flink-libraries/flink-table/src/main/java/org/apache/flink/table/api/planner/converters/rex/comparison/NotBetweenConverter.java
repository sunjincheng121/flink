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
import org.apache.flink.table.expressions.NotBetween;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * NotBetweenConverter.
 */
public class NotBetweenConverter {
	public static RexNode toRexNode(NotBetween notBetween, ExpressionVisitorImpl visitor) {
		return visitor.getRelBuilder().or(
			visitor.getRelBuilder().call(
				SqlStdOperatorTable.LESS_THAN,
				visitor.toRexNode(notBetween.expr()),
				visitor.toRexNode(notBetween.lowerBound())
			),
			visitor.getRelBuilder().call(
				SqlStdOperatorTable.GREATER_THAN,
				visitor.toRexNode(notBetween.expr()),
				visitor.toRexNode(notBetween.upperBound())
			)
		);
	}
}
