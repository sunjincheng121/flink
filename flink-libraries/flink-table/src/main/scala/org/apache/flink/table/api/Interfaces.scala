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
package org.apache.flink.table.api

import org.apache.flink.table.apiexpressions.{ApiExpression, ApiOverWindow, ApiWindow}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.types.Row

import _root_.scala.annotation.varargs


trait Table {
  def getSchema: TableSchema
  def printSchema(): Unit

  def selectApi(fields: Expression*): Table
  def select(fields: String): Table
  def select(fields: ApiExpression*): Table

  def createTemporalTableFunction(
      timeAttribute: String,
      primaryKey: String): TableFunction[Row]
  def createTemporalTableFunctionApi(
      timeAttribute: Expression,
      primaryKey: Expression): TableFunction[Row]
  def createTemporalTableFunction(
      timeAttribute: ApiExpression,
      primaryKey: ApiExpression): TableFunction[Row]


  def asApi(fields: Expression*): Table
  def as(fields: String): Table
  def as(fields: ApiExpression*): Table

  def filterApi(predicate: Expression): Table
  def filter(predicate: String): Table
  def filter(predicate: ApiExpression): Table

  def whereApi(predicate: Expression): Table
  def where(predicate: String): Table
  def where(predicate: ApiExpression): Table

  def groupByApi(fields: Expression*): GroupedTable
  def groupBy(fields: String): GroupedTable
  def groupBy(fields: ApiExpression*): GroupedTable

  def distinct(): Table

  def join(right: Table): Table
  def join(right: Table, joinPredicate: String): Table
  def joinApi(right: Table, joinPredicate: Expression): Table
  def join(right: Table, joinPredicate: ApiExpression): Table
  def leftOuterJoin(right: Table): Table
  def leftOuterJoin(right: Table, joinPredicate: String): Table
  def leftOuterJoinApi(right: Table, joinPredicate: Expression): Table
  def leftOuterJoin(right: Table, joinPredicate: ApiExpression): Table
  def rightOuterJoin(right: Table, joinPredicate: String): Table
  def rightOuterJoinApi(right: Table, joinPredicate: Expression): Table
  def rightOuterJoin(right: Table, joinPredicate: ApiExpression): Table
  def fullOuterJoin(right: Table, joinPredicate: String): Table
  def fullOuterJoinApi(right: Table, joinPredicate: Expression): Table
  def fullOuterJoin(right: Table, joinPredicate: ApiExpression): Table

  def minus(right: Table): Table
  def minusAll(right: Table): Table

  def union(right: Table): Table
  def unionAll(right: Table): Table

  def intersect(right: Table): Table
  def intersectAll(right: Table): Table

  def orderByApi(fields: Expression*): Table
  def orderBy(fields: String): Table
  def orderBy(fields: ApiExpression*): Table

  def offset(offset: Int): Table

  def fetch(fetch: Int): Table

  @deprecated("This method will be removed. Please register the TableSink and use " +
                "Table.insertInto().", "1.7.0")
  @Deprecated
  def writeToSink[T](sink: TableSink[T]): Unit
  @deprecated("This method will be removed. Please register the TableSink and use " +
                "Table.insertInto().", "1.7.0")
  @Deprecated
  def writeToSink[T](sink: TableSink[T], conf: QueryConfig): Unit

  def insertInto(tableName: String): Unit
  def insertInto(tableName: String, conf: QueryConfig): Unit

  def window(window: ApiWindow): WindowedTable
  def window(window: Window): WindowedTable

  @varargs
  def window(overWindows: UnresolvedOverWindow*): OverWindowedTable
}

trait GroupedTable {
  def selectApi(fields: Expression*): Table
  def select(fields: String): Table
  def select(fields: ApiExpression*): Table
}

trait WindowedTable{
  def groupByApi(fields: Expression*): WindowGroupedTable
  def groupBy(fields: String): WindowGroupedTable
  def groupBy(fields: ApiExpression*): WindowGroupedTable
}

trait OverWindowedTable {
  def selectApi(fields: Expression*): Table
  def select(fields: String): Table
  def select(fields: ApiExpression*): Table
}

trait WindowGroupedTable {
  def selectApi(fields: Expression*): Table
  def select(fields: String): Table
  def select(fields: ApiExpression*): Table
}
