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
package org.apache.flink.api.table.functions.utils

import java.util.concurrent.locks.ReentrantLock

import java.util.concurrent.locks.Lock
import org.apache.flink.api.table.Row

/**
  * Created by jincheng.sunjc on 16/12/8.
  */
object TempStore {

  var lMap: Map[String, List[Row]] = Map[String, List[Row]]()
  var rMap: Map[String, List[Row]] = Map[String, List[Row]]()
  val lock: Lock = new ReentrantLock()

  def lput(k: String, v: Row): Unit = {
    try {
      lock.lock()
      if (!lMap.contains(k)) {
        lMap += (k -> List[Row](v))
      }
      else {
        val data:List[Row] = lMap.get(k).get
        lMap += (k -> (data :+ v))
      }
    } finally {
      lock.unlock()
    }
  }

  def rput(k: String, v: Row): Unit = {
    try {
      lock.lock()
      if (!rMap.contains(k)) {
        rMap += (k -> List[Row](v))
      } else {
        val data:List[Row] = rMap.get(k).get
        rMap += (k -> (data :+ v))
      }
    } finally {
      lock.unlock()
    }

  }

  def lget(k: String): Option[List[Row]] = {
    try {
      lock.lock()
      lMap.get(k)
    } finally {
      lock.unlock()
    }

  }

  def rget(k: String): Option[List[Row]] = {
    try {
      lock.lock()
      rMap.get(k)
    } finally {
      lock.unlock()
    }

  }
}
