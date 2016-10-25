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

import org.apache.calcite.sql.SqlFunction
import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.{FlinkTypeFactory, TableException}
import org.apache.flink.api.table.functions.utils.TableValuedSqlFunction
import org.apache.flink.api.table.plan.schema.TableValuedFunctionImpl
import java.lang.reflect.Method

import org.apache.flink.api.table.expressions.{Expression}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Base class for a user-defined table valued function (UDTVF). A
  * user-defined table functions works on
  * one row as input and returns multiple rows as output.
  *
  * The behavior of a [[TableValuedFunction]] can be defined by implementing a custom evaluation
  * method. An evaluation method must be declared publicly and named "eval". Evaluation methods
  * can also be overloaded by implementing multiple methods named "eval".
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  *
  * By default the result type of an evaluation method is determined by Flink's type extraction
  * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
  * complex, custom, or composite types. In these cases [[TypeInformation]] of the result type
  * can be manually defined by overriding [[UserDefinedFunction.getResultType()]].
  *
  * Internally, the Table/SQL API code generation works with primitive values as much as possible.
  * If a user-defined table function should not introduce much overhead during runtime, it is
  * recommended to declare parameters and result types as primitive types instead of their boxed
  * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
  *
  * @tparam T The type of the output row
  */
abstract class TableValuedFunction[T] extends UserDefinedFunction {

  private var fieldNames: Array[String] = null
  private var fieldTypes: Array[TypeInformation[_]] = null
  private var fieldIndexes: Array[Int] = null

  def getFieldNames: Array[String] = {
    if (null == fieldNames) {
      getFieldInfo
    }
    fieldNames
  }

  def getFieldTypes: Array[TypeInformation[_]] = {
    if (null == fieldTypes) {
      getFieldInfo
    }
    fieldTypes
  }

  def getFieldIndexes: Array[Int] = {
    if (null == fieldIndexes) {
      getFieldInfo
    }
    fieldIndexes
  }

  def getFieldInfo: Unit = {
    fieldNames = getResultType match {
      case t: TupleTypeInfo[_] => t.getFieldNames
      case c: CaseClassTypeInfo[_] => c.getFieldNames
      case p: PojoTypeInfo[_] => p.getFieldNames
      case a: AtomicType[_] => Array("f0")
      case tpe =>
        throw new TableException(s"Type $tpe lacks explicit field naming")
    }
    fieldIndexes = fieldNames.indices.toArray
    fieldTypes = fieldNames.map { i =>
      getResultType match {
        case t: TupleTypeInfo[T] => t.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case c: CaseClassTypeInfo[T] => c.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case p: PojoTypeInfo[T] => p.getTypeAt(i).asInstanceOf[TypeInformation[_]]
        case a: AtomicType[T] => a.asInstanceOf[TypeInformation[_]]
        case tpe =>
          throw new TableException(s"Type $tpe lacks explicit field naming")
      }
    }
  }

  private var resultType: TypeInformation[T] = null

  /**
    * Returns the result type of the evaluation method with Generics information
    *
    * This method needs to be overriden in case Flink's type extraction facilities are not
    * sufficient to extract the [[TypeInformation]] based on the return type of the evaluation
    * method. Flink's type extraction facilities can handle basic types or
    * simple POJOs but might be wrong for more complex, custom, or composite types.
    *
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType: TypeInformation[T] = this.resultType

  def setResultType(resultType: TypeInformation[T]): Unit = {
    this.resultType = resultType
  }
  private[flink] final def createSqlFunction(
                                              name: String,
                                              method: Method,
                                              typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    val tableFunction = new TableValuedFunctionImpl(getResultType,
      getFieldIndexes, getFieldNames,method)
    val function = TableValuedSqlFunction(name, this, getResultType,
      typeFactory, tableFunction)
    function
  }


}
