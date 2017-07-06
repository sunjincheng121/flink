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

import _root_.java.util.HashMap

import org.apache.flink.api.common.time.Time
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.plan.schema.FlinkTable

class SourceConfig private[table] extends Serializable {}

/**
  * The [[BatchSourceConfig]] holds parameters to configure the behavior of batch source.
  */
class BatchSourceConfig private[table] extends SourceConfig

/**
  * The [[StreamSourceConfig]] holds parameters to configure the behavior of streaming source.
  */
class StreamSourceConfig private[table] extends SourceConfig {
  /**
    * A configuration item, for both [[org.apache.flink.streaming.api.datastream.DataStreamSource]]
    * and [[org.apache.flink.table.sources.StreamTableSource]], which defines the SLA for data
    * latency.
    *
    * __Note__: The value of lateDataTimeOffset can not be negative, negative values can cause
    * data loss.
    */
  private var lateDataTimeOffset: Long = 0L

  /**
    * Specifies the lateDataTimeOffset for define the data delay SLA. For example, if
    * lateDtaTimeOffset = 5 seconds, that meant all the data which delay is no
    * more than 5 seconds will be process correctly.
    *
    * __Note__: The value of lateDataTimeOffset can not be negative, negative values can cause
    * data loss.
    */
  def withLateDataTimeOffset(lateDataTimeOffset: Time): StreamSourceConfig = {

    if (0 > lateDataTimeOffset.toMilliseconds) {
      throw new IllegalArgumentException(
        s"The lateDataTimeOffset value is [${lateDataTimeOffset.toMilliseconds}], " +
          s"lateDataTimeOffset may not be smaller than 0.")
    }

    this.lateDataTimeOffset = lateDataTimeOffset.toMilliseconds
    this
  }

  def getLateDataTimeOffset: Long = {
    lateDataTimeOffset
  }

}

object SourceConfig {
  val sourceConfigCatalog = new HashMap[Either[TableSource[_], FlinkTable[_]], SourceConfig]

  private[table] def registerSourceConfig(
      item: Any,
      sourceConfig: SourceConfig): Unit = {
    item match {
      case source: TableSource[_] =>
        sourceConfigCatalog.put(Left(source), sourceConfig)
      case table: FlinkTable[_] =>
        sourceConfigCatalog.put(Right(table), sourceConfig)
      case _ =>
        throw new TableException(
          s"Only TableSource or FlinkTable can using SourceConfig, but get [${item}]")
    }

  }

  private[table] def getSourceConfig(item: Any): Option[SourceConfig] = {
    val key = item match {
      case source: TableSource[_] =>
        Left(source)
      case table: FlinkTable[_] =>
        Right(table)
      case _ =>
        throw new TableException(
          s"Only TableSource or FlinkTable can using SourceConfig, but get [${item}]")
    }
    if (sourceConfigCatalog.containsKey(key)) {
      Some(sourceConfigCatalog.get(key))
    } else {
      None
    }
  }
}

object StreamSourceConfig {
  def sourceConfig: StreamSourceConfig = new StreamSourceConfig
}

object BatchSourceConfig {
  def sourceConfig: BatchSourceConfig = new BatchSourceConfig
}
