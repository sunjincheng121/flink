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

import org.apache.calcite.converters.expression.AggregationConverter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Max;

public class MaxConverter implements AggregationConverter<Max> {
    @Override
    public RelBuilder.AggCall toAggCall(Max agg, String name, boolean isDistinct, RelBuilder relBuilder) {
        return relBuilder.aggregateCall(
            SqlStdOperatorTable.MAX,
            isDistinct,
            false,
            null,
            name,
            agg.child().toRexNode(relBuilder));
    }

    @Override
    public SqlAggFunction getSqlAggFunction(Max agg, RelBuilder relBuilder) {
        return SqlStdOperatorTable.MAX;
    }
}
