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
package org.apache.flink.table.api.scala.stream.bayes

import java.util.HashMap

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.sources.{DefinedRowtimeAttribute, StreamTableSource}
import org.apache.flink.types.Row


/**
  * Bayes 流数据源抽象类,主要集成ts提取和watermark生成策略的支持.
  */
abstract class AbstractStreamSource extends StreamTableSource[Row] with DefinedRowtimeAttribute {
  var currentQueryWaterMarkConfigName: String = null
  var queryWaterMarkConfigs = new HashMap[String, QueryWaterMarkConfig]()

  def setCurrentQueryWaterMarkConfigName(name: String): Unit = {
    if (queryWaterMarkConfigs.containsKey(name)) {
      this.currentQueryWaterMarkConfigName = name
    } else {
      throw new RuntimeException(
        s"Did not find the corresponding configuration which named " +
          s"${currentQueryWaterMarkConfigName}")
    }

  }

  def addQueryWaterMarkConfig(queryWaterMarkConfig: QueryWaterMarkConfig) = {
    this.queryWaterMarkConfigs.put(queryWaterMarkConfig.columnName, queryWaterMarkConfig)
  }

  def getQueryWaterMarkConfig: QueryWaterMarkConfig = {
    if (!queryWaterMarkConfigs.isEmpty) {
      if (null == currentQueryWaterMarkConfigName) {
        queryWaterMarkConfigs.entrySet().iterator().next().getValue
      } else {
        queryWaterMarkConfigs.get(currentQueryWaterMarkConfigName)
      }
    } else {
      throw new RuntimeException("Please add at least one queryWaterMarkConfig.")
    }
  }

  override def getRowtimeAttribute: String = getQueryWaterMarkConfig.columnName

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {

    val config = getQueryWaterMarkConfig
    config.configType match {
      case 0 =>
        execEnv
        .addSource(genSourceFunction)
        .assignTimestampsAndWatermarks(new BayesAssignerWithPunctuatedWatermarks(config.gen))
        .returns(getReturnType)
      case _ =>
        throw new RuntimeException(
          s"Unsupported queryWaterMarkConfig type [${config.configType}]")
    }

  }

  def genSourceFunction: SourceFunction[Row]

}

