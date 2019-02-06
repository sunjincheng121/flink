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

package org.apache.flink.table

import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.DataSet
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{ImplicitExpressionConversions, DataSetConversions, DataStreamConversions, TableConversions, BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}

import _root_.scala.language.implicitConversions

package object expressions extends ImplicitExpressionConversions {

  implicit def table2TableConversions(table: Table): TableConversions = {
    new TableConversions(table)
  }

  implicit def dataSet2DataSetConversions[T](set: DataSet[T]): DataSetConversions[T] = {
    new DataSetConversions[T](set, set.getType())
  }

  implicit def table2RowDataSet(table: Table): DataSet[Row] = {
    val tableEnv = table.tableEnv.asInstanceOf[ScalaBatchTableEnv]
    tableEnv.toDataSet[Row](table)
  }

  implicit def dataStream2DataStreamConversions[T](set: DataStream[T]): DataStreamConversions[T] = {
    new DataStreamConversions[T](set, set.dataType)
  }

  implicit def table2RowDataStream(table: Table): DataStream[Row] = {
    val tableEnv = table.tableEnv.asInstanceOf[ScalaStreamTableEnv]
    tableEnv.toAppendStream[Row](table)
  }

}
