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

import org.apache.flink.types.Row

/**
  * 用户需要实现的提取ts和生成watermark的逻辑
  */
trait GenTsAndWatermark extends Serializable {
  /**
    * 提取ts逻辑
    *
    * @param row                      数据行
    * @param previousElementTimestamp 上一行数据的ts
    * @return 返回当前行的ts
    */
  def extractTs(row: Row, previousElementTimestamp: Long): Long

  /**
    * 生成watermark
    *
    * @param row                数据行
    * @param extractedTimestamp 提取的ts值和extractTs返回相等
    * @return 返回用于生成watermark的long值
    */
  def genWatermark(row: Row, extractedTimestamp: Long): Long
}
