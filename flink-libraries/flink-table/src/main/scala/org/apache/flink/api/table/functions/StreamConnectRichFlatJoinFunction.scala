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
package org.apache.flink.api.table.functions

import org.apache.flink.api.common.functions.RichFlatJoinFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.table.Row

/**
  * Created by jincheng.sunjc on 16/12/8.
  */
class StreamConnectRichFlatJoinFunction[L, R, O] extends RichFlatJoinFunction[L, R, O] {

  override def join(first: L, second: R, out: Collector[O]): Unit = {
    val left = first.asInstanceOf[Row]
    val right = second.asInstanceOf[Row]

    val output: Row = new Row(left.productArity + right.productArity)
    for (i <- 0 until left.productArity) {
      output.setField(i, left.productElement(i))
    }
    for (i <- 0 until right.productArity) {
      output.setField(i + left.productArity, right.productElement(i))
    }
    out.collect(output.asInstanceOf[O])
  }
}
