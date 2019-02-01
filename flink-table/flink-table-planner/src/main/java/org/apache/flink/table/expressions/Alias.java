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

import java.util.ArrayList;
import java.util.List;

/**
 * Alias.
 */
public class Alias extends UnaryExpression {

	public static Alias apply(Expression child, String name, List<String> extraNames) {
		return new Alias(child, name, extraNames);
	}

	public static Alias apply(Expression child, String name) {
		return new Alias(child, name);
	}

	private Expression child;
	private String name;
	private List<String> extraNames;

	private Alias(Expression child, String name, List<String> extraNames) {
		this.child = child;
		this.name = name;
		this.extraNames = new ArrayList<>(extraNames);
	}

	private Alias(Expression child, String name) {
		this.child = child;
		this.name = name;
		this.extraNames = new ArrayList<>();
	}

	@Override
	Expression getChild() {
		return child;
	}

	String getName() {
		return name;
	}

	List<String> getExtraNames() {
		return extraNames;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visitAlias(this);
	}
}
