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

/**
 * The visitor definition of expression. ExpressionVisitor is the transformation mechanism of
 * api module expression to planner module expression.
 *
 * @param <R> The type of result expression.
 */
public interface ExpressionVisitor<R> {

	R visitCall(Call call);

	R visitUnresolvedOverCall(UnresolvedOverCall overCall);

	R visitDistinctAgg(DistinctAgg distinctAgg);

	R visitAlias(Alias alias);

	R visitRowtimeAttribute(RowtimeAttribute rowtimeAttribute);

	R visitProctimeAttribute(ProctimeAttribute proctimeAttribute);

	R visitTypeLiteral(TypeLiteral typeLiteral);

	R visitUnboundedRange(UnboundedRange unboundedRange);

	R visitCurrentRange(CurrentRange currentRange);

	R visitNull(Null nullExpression);

	R visitSymbolExpression(SymbolExpression symbolExpression);

	R visitLiteral(Literal literal);

	R visitCurrentRow(CurrentRow currentRow);

	R visitTableReference(TableReference tableReference);

	R visitUnboundedRow(UnboundedRow unboundedRow);

	R visitUnresolvedFieldReference(UnresolvedFieldReference unresolvedFieldReference);
}
