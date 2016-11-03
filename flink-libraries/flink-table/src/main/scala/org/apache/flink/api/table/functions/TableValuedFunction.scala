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
import org.apache.flink.api.common.typeinfo.TypeInformation
trait TableValuedFunction[T]  {
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
  def getResultType(signature: Array[Class[_]]): TypeInformation[_]  = null

}