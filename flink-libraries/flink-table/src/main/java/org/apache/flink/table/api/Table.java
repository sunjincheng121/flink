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

package org.apache.flink.table.api;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.sinks.TableSink;

/**
 * Table.
 */
public interface Table {
	TableSchema getSchema();

	void printSchema();

	Table select(Expression ... fields);

	Table select(String fields);

	TemporalTableFunction createTemporalTableFunction(String timeAttribute , String primaryKey);

	TemporalTableFunction createTemporalTableFunction(Expression timeAttribute, Expression primaryKey);

	Table as(Expression ... fields);

	Table as(String fields);

	Table filter(Expression predicate);

	Table filter(String predicate);

	Table where(Expression predicate);

	Table where(String predicate);

	GroupedTable groupBy(Expression ... fields);

	GroupedTable groupBy(String fields);

	Table distinct();

	Table join(Table right);

	Table join(Table right, String joinPredicate);

	Table join(Table right, Expression joinPredicate);

	Table leftOuterJoin(Table right);

	Table leftOuterJoin(Table right, String joinPredicate);

	Table leftOuterJoin(Table right, Expression joinPredicate);

	Table rightOuterJoin(Table right, String joinPredicate);

	Table rightOuterJoin(Table right, Expression joinPredicate);

	Table fullOuterJoin(Table right, String joinPredicate);

	Table fullOuterJoin(Table right, Expression joinPredicate);

	Table minus(Table right);

	Table minusAll(Table right);

	Table union(Table right);

	Table unionAll(Table right);

	Table intersect(Table right);

	Table intersectAll(Table right);

	Table orderBy(Expression ... fields);

	Table orderBy(String fields);

	Table offset(int offset);

	Table fetch(int fetch);

	<T> void writeToSink(TableSink<T> sink);

	<T> void writeToSink(TableSink<T> sink, QueryConfig conf);

	void insertInto(String tableName);

	void insertInto(String tableName, QueryConfig conf);

	WindowedTable window(Window window);

	OverWindowedTable window(OverWindow ...overWindows);
}
