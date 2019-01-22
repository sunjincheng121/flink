/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.converters.expression.aggregations;

import org.apache.calcite.converters.expression.AggregationConverter3;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.visitor.RexNodeVisitorImpl;
import org.apache.flink.table.expressions.Count;

public class CountConverter implements AggregationConverter3<Count> {
    @Override
    public RelBuilder.AggCall toAggCall(Count agg, String name, boolean isDistinct, RelBuilder relBuilder) {

        return relBuilder.aggregateCall(
                SqlStdOperatorTable.COUNT,
                isDistinct,
                false,
                null,
                name,
                agg.child().accept(new RexNodeVisitorImpl(relBuilder)));
    }

    @Override
    public SqlAggFunction getSqlAggFunction(Count agg, RelBuilder relBuilder) {
        return SqlStdOperatorTable.COUNT;
    }
}
