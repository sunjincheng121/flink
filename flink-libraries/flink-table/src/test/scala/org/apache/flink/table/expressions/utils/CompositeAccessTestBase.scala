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

package org.apache.flink.table.expressions.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.expressions.utils.CompositeAccessTestBase.{MyCaseClass, MyCaseClass2, MyPojo}
import org.apache.flink.types.Row

class CompositeAccessTestBase extends ExpressionTestBase {

  def testData = {
    val testData = new Row(8)
    testData.setField(0, MyCaseClass(42, "Bob", booleanField = true))
    testData.setField(1, MyCaseClass2(MyCaseClass(25, "Timo", booleanField = false)))
    testData.setField(2, ("a", "b"))
    testData.setField(3, new org.apache.flink.api.java.tuple.Tuple2[String, String]("a", "b"))
    testData.setField(4, new MyPojo())
    testData.setField(5, 13)
    testData.setField(6, MyCaseClass2(null))
    testData.setField(7, Tuple1(true))
    testData
  }

  def typeInfo = {
    new RowTypeInfo(
      createTypeInformation[MyCaseClass],
      createTypeInformation[MyCaseClass2],
      createTypeInformation[(String, String)],
      new TupleTypeInfo(Types.STRING, Types.STRING),
      TypeExtractor.createTypeInfo(classOf[MyPojo]),
      Types.INT,
      createTypeInformation[MyCaseClass2],
      createTypeInformation[Tuple1[Boolean]]
      ).asInstanceOf[TypeInformation[Any]]
  }

}

object CompositeAccessTestBase {
  case class MyCaseClass(intField: Int, stringField: String, booleanField: Boolean)

  case class MyCaseClass2(objectField: MyCaseClass)

  class MyPojo {
    private var myInt: Int = 0
    private var myString: String = "Hello"

    def getMyInt = myInt

    def setMyInt(value: Int) = {
      myInt = value
    }

    def getMyString = myString

    def setMyString(value: String) = {
      myString = myString
    }
  }
}
