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
import org.apache.flink.api.table.{FlinkTypeFactory, ValidationException}
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils.checkForInstantiation
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils._

import scala.collection.mutable

/**
  * Base class for all user-defined functions such as scalar functions, table functions,
  * or aggregation functions.
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  */
abstract class UserDefinedFunction {

  // we cache SQL functions to reduce amount of created objects
  // (i.e. for type inference, validation, etc.)
  private val cachedSqlFunctions = mutable.HashMap[String, SqlFunction]()

  private val evalMethods = checkAndExtractEvalMethods()

  private lazy val signatures = evalMethods.map(_.getParameterTypes)

  /**
    * Extracts evaluation methods and throws a [[ValidationException]] if no implementation
    * can be found.
    */
  private def checkAndExtractEvalMethods(): Array[Method] = {
    val methods = getClass
      .getDeclaredMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == "eval" && Modifier.isPublic(modifiers) && !Modifier.isAbstract(modifiers)
      }

    if (methods.isEmpty) {
      throw new ValidationException(s"Table function class '$this' does not implement at least " +
        s"one method named 'eval' which is public and not abstract.")
    } else {
      methods
    }
  }

  /**
    * Returns all found evaluation methods of the possibly overloaded function.
    */
  private[flink] final def getEvalMethods: Array[Method] = evalMethods

  /**
    * Returns all found signature of the possibly overloaded function.
    */
  private[flink] final def getSignatures: Array[Array[Class[_]]] = signatures

  // check if function can be instantiated
  checkForInstantiation(this.getClass)

  /**
    * Returns the corresponding [[SqlFunction]]. Creates an instance if not already created.
    */
  private[flink] final def getSqlFunction(
                                           name: String,
                                           typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    cachedSqlFunctions.getOrElseUpdate(name, createSqlFunctions(name, typeFactory)(0))
  }
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the result type of the evaluation method with a given signature.
    *
    * This method needs to be overriden in case Flink's type extraction facilities are not
    * sufficient to extract the [[TypeInformation]] based on the return type of the evaluation
    * method. Flink's type extraction facilities can handle basic types or
    * simple POJOs but might be wrong for more complex, custom, or composite types.
    *
    * @param signature signature of the method the return type needs to be determined
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    val evalMethod = this.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))
    try {
      TypeExtractor.getForClass(evalMethod.getReturnType)
    } catch {
      case ite: InvalidTypesException =>
        throw new ValidationException(s"Return type of scalar function '$this' cannot be " +
          s"automatically determined. Please provide type information manually.")
    }
  }
  /**
    * Returns [[TypeInformation]] about the operands of the evaluation method with a given
    * signature.
    *
    * In order to perform operand type inference in SQL (especially when NULL is used) it might be
    * necessary to determine the parameter [[TypeInformation]] of an evaluation method.
    * By default Flink's type extraction facilities are used for this but might be wrong for
    * more complex, custom, or composite types.
    *
    * @param signature signature of the method the operand types need to be determined
    * @return [[TypeInformation]] of  operand types
    */
  def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.map { c =>
      try {
        TypeExtractor.getForClass(c)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of scalar function '$this' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }

  /**
    * Creates corresponding [[SqlFunction]].
    */
  private[flink] def createSqlFunction(
                                        name: String,
                                        method:Method,
                                        typeFactory: FlinkTypeFactory)
  : SqlFunction

  private[flink] final def createSqlFunctions(
                                               name: String,
                                               typeFactory: FlinkTypeFactory)
  : Array[SqlFunction] = {
    getEvalMethods.map(method => {
      createSqlFunction(name, method, typeFactory)
    })
  }
  override def toString = getClass.getCanonicalName
}
