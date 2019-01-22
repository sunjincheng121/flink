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

package org.apache.flink.table.api.planner.converters.rex.subquery;

import org.apache.flink.table.api.InnerTable;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.expressions.TableReference;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * InConverter.
 */
public class InConverter {
	public static RexNode toRexNode(In in, ExpressionVisitorImpl visitor) {
		// check if this is a sub-query expression or an element list
		if (in.elements().head() instanceof TableReference) {

			TableReference tableReference = (TableReference) in.elements().head();
			String name = tableReference.name();
			InnerTable table = (InnerTable) tableReference.table();
			return RexSubQuery.in(table.getRelNode(),
				ImmutableList.of(visitor.toRexNode(in.expression())));

		} else {

			return visitor.toRexNode(SqlStdOperatorTable.IN, in.children());

		}
	}
}
