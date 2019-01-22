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

package org.apache.flink.table.api.planner.converters.rex;

import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Cast;

import org.apache.calcite.rex.RexNode;

/**
 * CastRexConverter.
 */
public class CastRexConverter {
	public static RexNode toRexNode(Cast expr, ExpressionVisitorImpl visitor) {
		RexNode childRexNode =  expr.child().accept(visitor);
		FlinkTypeFactory typeFactory = (FlinkTypeFactory) visitor.getRelBuilder().getTypeFactory();
		return visitor.getRelBuilder().getRexBuilder().makeAbstractCast(
				typeFactory.createTypeFromTypeInfo(
						expr.resultType(),
						childRexNode.getType().isNullable()), childRexNode);
	}
}
