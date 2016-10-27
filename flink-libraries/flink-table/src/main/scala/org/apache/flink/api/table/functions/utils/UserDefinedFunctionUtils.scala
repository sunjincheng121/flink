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

import java.lang.reflect.{Method, Modifier, ParameterizedType}
import java.sql.{Date, Time, Timestamp}

import com.google.common.primitives.Primitives
import org.apache.calcite.sql.SqlFunction
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.table.{FlinkTypeFactory, TableException, ValidationException}
import org.apache.flink.api.table.functions.{ScalarFunction, TableValuedFunction, UserDefinedFunction}
import org.apache.flink.api.table.plan.schema.TableValuedFunctionImpl
import org.apache.flink.util.InstantiationUtil

object UserDefinedFunctionUtils {

  /**
    * Instantiates a user-defined function.
    */
  def instantiate[T <: UserDefinedFunction](clazz: Class[T]): T = {
    val constructor = clazz.getDeclaredConstructor()
    constructor.setAccessible(true)
    constructor.newInstance()
  }

  /**
    * Checks if a user-defined function can be easily instantiated.
    */
  def checkForInstantiation(clazz: Class[_]): Unit = {
    if (!InstantiationUtil.isPublic(clazz)) {
      throw ValidationException("Function class is not public.")
    }
    else if (!InstantiationUtil.isProperClass(clazz)) {
      throw ValidationException("Function class is no proper class, it is either abstract," +
        " an interface, or a primitive type.")
    }
    else if (InstantiationUtil.isNonStaticInnerClass(clazz)) {
      throw ValidationException("The class is an inner class, but not statically accessible.")
    }

    // check for default constructor (can be private)
    clazz
      .getDeclaredConstructors
      .find(_.getParameterTypes.isEmpty)
      .getOrElse(throw ValidationException("Function class needs a default constructor."))
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for ScalarFunction
  // ----------------------------------------------------------------------------------------------

  /**
    * Prints one signature consisting of classes.
    */
  def signatureToString(signature: Array[Class[_]]): String =
  "(" + signature.map { clazz =>
    if (clazz == null) {
      "null"
    } else {
      clazz.getCanonicalName
    }
  }.mkString(", ") + ")"

  /**
    * Prints one signature consisting of TypeInformation.
    */
  def signatureToString(signature: Seq[TypeInformation[_]]): String = {
    signatureToString(typeInfoToClass(signature))
  }

  /**
    * Extracts type classes of [[TypeInformation]] in a null-aware way.
    */
  def typeInfoToClass(typeInfos: Seq[TypeInformation[_]]): Array[Class[_]] =
  typeInfos.map { typeInfo =>
    if (typeInfo == null) {
      null
    } else {
      typeInfo.getTypeClass
    }
  }.toArray


  /**
    * Compares parameter candidate classes with expected classes. If true, the parameters match.
    * Candidate can be null (acts as a wildcard).
    */
  def parameterTypeEquals(candidate: Class[_], expected: Class[_]): Boolean =
  candidate == null ||
    candidate == expected ||
    expected.isPrimitive && Primitives.wrap(expected) == candidate ||
    candidate == classOf[Date] && expected == classOf[Int] ||
    candidate == classOf[Time] && expected == classOf[Int] ||
    candidate == classOf[Timestamp] && expected == classOf[Long]

  /**
    * Returns signatures matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getSignature(
                    useDefinedFunction: UserDefinedFunction,
                    signature: Seq[TypeInformation[_]])
  : Option[Array[Class[_]]] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)

    useDefinedFunction
      .getSignatures
      // go over all signatures and find one matching actual signature
      .find { curSig =>
      // match parameters of signature to actual parameters
      actualSignature.length == curSig.length &&
        curSig.zipWithIndex.forall { case (clazz, i) =>
          parameterTypeEquals(actualSignature(i), clazz)
        }
    }
  }
  /**
    * Returns signatures for obj class
    */
  def getSignatures(clazz:Class[_]): Array[Array[Class[_]]] = checkAndExtractEvalMethods(clazz).map(_.getParameterTypes)
  /**
    * Returns signatures matching the given signature of [[TypeInformation]].
    * Elements of the signature can be null (act as a wildcard).
    */
  def getSignature(
                    clazz:Class[_],
                    signature: Seq[TypeInformation[_]])
  : Option[Array[Class[_]]] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)
    getSignatures(clazz)
      // go over all signatures and find one matching actual signature
      .find { curSig =>
      // match parameters of signature to actual parameters
      actualSignature.length == curSig.length &&
        curSig.zipWithIndex.forall { case (clazz, i) =>
          parameterTypeEquals(actualSignature(i), clazz)
        }
    }
  }
  /**
    * Internal method of [[ScalarFunction#getResultType()]] that does some pre-checking and uses
    * [[TypeExtractor]] as default return type inference.
    */
  def getResultType(
                     scalarFunction: ScalarFunction,
                     signature: Array[Class[_]])
  : TypeInformation[_] = {
    // find method for signature
    val evalMethod = scalarFunction.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new ValidationException("Given signature is invalid."))

    val userDefinedTypeInfo = scalarFunction.getResultType(signature)
    if (userDefinedTypeInfo != null) {
      userDefinedTypeInfo
    } else {
      try {
        TypeExtractor.getForClass(evalMethod.getReturnType)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(s"Return type of scalar function '$this' cannot be " +
            s"automatically determined. Please provide type information manually.")
      }
    }
  }


  /**
    * Returns the return type of the evaluation method matching the given signature.
    */
  def getResultTypeClass(
                          scalarFunction: ScalarFunction,
                          signature: Array[Class[_]])
  : Class[_] = {
    // find method for signature
    val evalMethod = scalarFunction.getEvalMethods
      .find(m => signature.sameElements(m.getParameterTypes))
      .getOrElse(throw new IllegalArgumentException("Given signature is invalid."))
    evalMethod.getReturnType
  }

  /**
    * Prints all signatures of a [[ScalarFunction]].
    */
  def signaturesToString(userDefinedFunction: UserDefinedFunction): String = {
    userDefinedFunction.getSignatures.map(signatureToString).mkString(", ")
  }

  /**
    * Returns eval method matching the given signature of [[TypeInformation]].
    */
  def getEvalMethod(
                     function: UserDefinedFunction,
                     signature: Seq[TypeInformation[_]])
  : Option[Method] = {
    // We compare the raw Java classes not the TypeInformation.
    // TypeInformation does not matter during runtime (e.g. within a MapFunction).
    val actualSignature = typeInfoToClass(signature)

    function
      .getEvalMethods
      // go over all eval methods and find one matching
      .find { cur =>
      val signatures = cur.getParameterTypes
      // match parameters of signature to actual parameters
      actualSignature.length == signatures.length &&
        signatures.zipWithIndex.forall { case (clazz, i) =>
          parameterTypeEquals(actualSignature(i), clazz)
        }
    }
  }

  // ----------------------------------------------------------------------------------------------
  // Utilities for TableValuedFunction
  // ----------------------------------------------------------------------------------------------

  /**
    * Extracts evaluation methods and throws a [[ValidationException]] if no implementation
    * can be found.
    */
  def checkAndExtractEvalMethods(clazz: Class[_]): Array[Method] = {
    val methods = clazz
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
    * Returns the result type of the evaluation method with a given signature.
    * @param signature signature of the method the return type needs to be determined
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType(
                     clazz: Class[_],
                     signature: Array[Class[_]])
  : TypeInformation[_] = {
    val evalMethod = checkAndExtractEvalMethods(clazz)
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
    * Creates corresponding [[SqlFunction]].
    */
  def createSqlFunction[T: TypeInformation](
                         name: String,
                         method: Method,
                         obj: Object,
                         typeFactory: FlinkTypeFactory)
  : SqlFunction = {
    val clazz = obj.getClass
    obj match {
      case tf: TableValuedFunction[_] =>
        val resultType = implicitly[TypeInformation[T]]
        val (fieldNames, fieldIndexs): (Array[String], Array[Int]) = getFieldInfo(resultType)
        val tableFunction = new TableValuedFunctionImpl(resultType,
          fieldIndexs, fieldNames, method)
        val function = TableValuedSqlFunction(name, obj.asInstanceOf[TableValuedFunction[_]], resultType,
          typeFactory, tableFunction)
        function
      case _ => throw new TableException("Unsupported user-defined function type.")

    }

  }

  /**
    * Creates all corresponding [[SqlFunction]].
    */
  def createSqlFunctions[T: TypeInformation](name: String,
                            obj: Object,
                            typeFactory: FlinkTypeFactory): Array[SqlFunction] = checkAndExtractEvalMethods(obj.getClass).map(m => {
    createSqlFunction(name, m, obj, typeFactory)
  })


  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * Field names are automatically extracted for
    * [[org.apache.flink.api.common.typeutils.CompositeType]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo(inputType: TypeInformation[_])
  : (Array[String], Array[Int]) = {
    val fieldNames: Array[String] = inputType match {
      case t: TupleTypeInfo[_] => t.getFieldNames
      case c: CaseClassTypeInfo[_] => c.getFieldNames
      case p: PojoTypeInfo[_] => p.getFieldNames
      case a: AtomicType[_] => Array("f0")
      case tpe =>
        throw new TableException(s"Type $tpe lacks explicit field naming")
    }
    val fieldIndexes = fieldNames.indices.toArray
    (fieldNames, fieldIndexes)
  }

  /**
    * Conver java.util.Iterator to scala.collection.Iterator when codegen
    */
  def jiter2siter[T](iter: Object): scala.collection.Iterator[T] ={
    iter match {
      case x : java.util.Iterator[T] => new scala.collection.Iterator[T] {
        def hasNext(): Boolean = x.hasNext

        def next(): T = x.next

        def remove(): Unit = throw new UnsupportedOperationException()
      }
      case _ => iter.asInstanceOf[(scala.collection.Iterator[T])]
    }

  }
}
