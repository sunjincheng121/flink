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

import org.apache.flink.annotation.PublicEvolving;

import java.util.List;
import java.util.Optional;

/**
 * The table function call.
 *
 * <p>
 * NOTE: this function call is only a temporary solution until we remove the
 * deprecated table constructor.
 * </p>
 */
@PublicEvolving
public final class TableFunctionCall extends Call {

	private Optional<String[]> alias = Optional.empty();

	public TableFunctionCall(TableFunctionDefinition func, List<Expression> args) {
		super(func, args);
	}

	public TableFunctionCall alias(String[] alias) {
		this.alias = Optional.of(alias);
		return this;
	}

	public Optional<String[]> alias() {
		return alias;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitCall(this);
	}
}
