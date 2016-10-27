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
package org.apache.flink.api.table.expressions.utils

import org.apache.flink.api.table.functions.TableValuedFunction

import scala.collection.mutable.ListBuffer

class TableValuedFunction0 extends TableValuedFunction[String] {

  def eval(str: String): Iterable[String] = {
    val rows: ListBuffer[String] = new ListBuffer
    if (str.contains("#")) {
      val items = str.split("#")
      for (item <- items)
        rows += item
    }
    rows
  }

  def eval(str: String, ignore: String): Iterable[String] = {
    val rows: ListBuffer[String] = new ListBuffer
    if (str.contains("#") && !str.contains(ignore)) {
      val items = str.split("#")
      for (item <- items)
        rows += item
    }
    rows
  }


}


case class Child(age: Int, name: String)

class TableValuedFunction1 extends TableValuedFunction[(Child)] {

  def eval(str: String): Iterable[Child] = {
    val rows: ListBuffer[Child] = new ListBuffer
    if (str.contains("#")) {
      val items = str.split("#")
      rows += Child(items(0).toInt, items(1))
    }
    rows
  }
}
