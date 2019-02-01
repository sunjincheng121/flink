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

package org.apache.flink.table.expressions;

import org.apache.flink.table.api.Table;

/**
 * TableReference.
 */
public class TableReference extends LeafExpression {

	public static TableReference apply(String name, Table table) {
		return new TableReference(name, table);
	}

	private String name;
	private Table table;

	private TableReference(String name, Table table) {
		this.name = name;
		this.table = table;
	}

	String getName() {
		return name;
	}

	Table getTable() {
		return table;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitTableReference(this);
	}
}
