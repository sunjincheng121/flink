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

package org.apache.flink.table.api.base.visitor;

import org.apache.flink.table.expressions.AggFunctionCall;
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

/**
 * The aggregation  visitor, define a visit method for each aggregation.
 *
 */
public interface AggregationVisitor<T> {
	T visit(DistinctAgg distinctAgg);

	T visit(Sum sum);

	T visit(Sum0 sum);

	T visit(Min min);

	T visit(Max max);

	T visit(Count count);

	T visit(Avg avg);

	T visit(Collect collect);

	T visit(StddevPop stddevPop);

	T visit(StddevSamp stddevSamp);

	T visit(VarPop varPop);

	T visit(VarSamp varSamp);

	T visit(AggFunctionCall aggFunctionCall);
}
