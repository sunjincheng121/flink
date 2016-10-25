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

import java.lang.reflect.{Method, Modifier}

import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.expressions.{Expression, ScalarFunctionCall}
import org.apache.flink.api.table.functions.utils.ScalarSqlFunction
import org.apache.flink.api.table.{FlinkTypeFactory, ValidationException}

/**
  * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
  * or multiple scalar values to a new scalar value.
  *
  * The behavior of a [[ScalarFunction]] can be defined by implementing a custom evaluation
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
  * If a user-defined scalar function should not introduce much overhead during runtime, it is
  * recommended to declare parameters and result types as primitive types instead of their boxed
  * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
  */abstract class ScalarFunction extends UserDefinedFunction {

  /**
    * Creates a call to a [[ScalarFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[Expression]] in form of a [[ScalarFunctionCall]]
    */
  final def apply(params: Expression*): Expression = {
    ScalarFunctionCall(this, params)
  }

  override private[flink] final def createSqlFunction(
                                                       name: String,
                                                       mothod:Method,
                                                       typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    new ScalarSqlFunction(name, this, typeFactory)
  }

}
