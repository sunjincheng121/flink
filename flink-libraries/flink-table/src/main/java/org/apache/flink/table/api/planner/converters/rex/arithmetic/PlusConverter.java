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

package org.apache.flink.table.api.planner.converters.rex.arithmetic;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.expressions.Cast;
import org.apache.flink.table.expressions.Plus;
import org.apache.flink.table.typeutils.TypeCheckUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * PlusConverter.
 */
public class PlusConverter {
	public static RexNode toRexNode(Plus plus, ExpressionVisitorImpl visitor) {
		if (TypeCheckUtils.isString(plus.left().resultType())) {
			Cast castedRight = Cast.apply(plus.right(), BasicTypeInfo.STRING_TYPE_INFO);

			return visitor.getRelBuilder().call(
				SqlStdOperatorTable.CONCAT,
				visitor.toRexNode(plus.left()),
				visitor.toRexNode(castedRight));

		} else if (TypeCheckUtils.isString(plus.right().resultType())) {
			Cast castedLeft = Cast.apply(plus.left(), BasicTypeInfo.STRING_TYPE_INFO);

			return visitor.getRelBuilder().call(
				SqlStdOperatorTable.CONCAT,
				visitor.toRexNode(castedLeft),
				visitor.toRexNode(plus.right()));

		} else if (TypeCheckUtils.isTimeInterval(plus.left().resultType())
			&& plus.left().resultType() == plus.right().resultType()) {

			return visitor.getRelBuilder().call(SqlStdOperatorTable.PLUS,
				visitor.toRexNode(plus.left()),
				visitor.toRexNode(plus.right()));

		} else if (TypeCheckUtils.isTimeInterval(plus.left().resultType())
			&& TypeCheckUtils.isTemporal(plus.right().resultType())) {

			// Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
			// we manually switch them here
			return visitor.getRelBuilder().call(
				SqlStdOperatorTable.DATETIME_PLUS,
				visitor.toRexNode(plus.right()),
				visitor.toRexNode(plus.left()));

		} else if (TypeCheckUtils.isTemporal(plus.left().resultType())
			&& TypeCheckUtils.isTemporal(plus.right().resultType())) {

			return visitor.getRelBuilder().call(
				SqlStdOperatorTable.DATETIME_PLUS,
				visitor.toRexNode(plus.left()),
				visitor.toRexNode(plus.right()));

		} else {

			Cast castedLeft = Cast.apply(plus.left(), plus.resultType());
			Cast castedRight = Cast.apply(plus.right(), plus.resultType());
			return visitor.getRelBuilder().call(
				SqlStdOperatorTable.PLUS,
				visitor.toRexNode(castedLeft),
				visitor.toRexNode(castedRight));

		}
	}
}
