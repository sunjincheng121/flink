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

package org.apache.flink.ml.common

import java.util.Random

import com.fasterxml.uuid.Generators
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Table
import org.apache.flink.table.functions.{AggregateFunction, FunctionContext, ScalarFunction}
import org.apache.flink.api.java.tuple

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object TableFlinkMLTools {
  def block[T: TypeInformation: ClassTag](
      input: Table, numBlocks: Int, partitionerOption: Option[ScalarFunction]): Table = {
    val blockIDAssigner = new BlockIDAssigner[T](numBlocks)
    val blockIDInput = input
      .as('data)
      .select(blockIDAssigner('data).flatten())
      .as('id, 'data)

    val preGroupBlockIDInput = partitionerOption match {
      case Some(partitioner) =>
        blockIDInput.select(partitioner('id, numBlocks) as 'id, 'data)

      case None => blockIDInput
    }

    val blockGenerator = new BlockGenerator[T]
    preGroupBlockIDInput.groupBy('id).select(blockGenerator('id, 'data))
  }

  object ModuloKeyPartitionFunction extends ScalarFunction {
    def eval(key: Int, numPartitions: Int): Int = {
      val result = key % numPartitions

      if(result < 0) {
        result + numPartitions
      } else {
        result
      }
    }

    def eval(key: String, numPartitions: Int): Int = {
      val result = key.hashCode % numPartitions

      if (result < 0) {
        result + numPartitions
      } else {
        result
      }
    }
  }
}

class BlockIDAssigner[T: TypeInformation](numBlocks: Int) extends ScalarFunction {
  def eval(element: T): (Int, T) = {
    val blockID = element.hashCode() % numBlocks

    val blockIDResult = if(blockID < 0){
      blockID + numBlocks
    } else {
      blockID
    }

    (blockIDResult, element)
  }

  def getTypeInfo[K: TypeInformation]: TypeInformation[K] = implicitly[TypeInformation[K]]

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    getTypeInfo[(Int, T)]
  }
}

class BlockGenerator[T] extends AggregateFunction[Block[T], tuple.Tuple2[Int, ArrayBuffer[T]]] {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  override def createAccumulator(): tuple.Tuple2[Int, ArrayBuffer[T]] =
    new tuple.Tuple2(0, ArrayBuffer[T]())

  def resetAccumulator(accumulator: tuple.Tuple2[Int, ArrayBuffer[T]]): Unit = {
    accumulator.f0 = 0
    accumulator.f1.clear()
  }

  def accumulate(acc: tuple.Tuple2[Int, ArrayBuffer[T]], id: Int, element: T): Unit = {
    acc.f0 = id
    acc.f1.append(element)
  }

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  override def getValue(accumulator: tuple.Tuple2[Int, ArrayBuffer[T]]): Block[T] = {
    Block[T](accumulator.f0, accumulator.f1.toVector)
  }
}


class BroadcastSingleElementMapperFunction[T, B, O](fun: (T, B) => O) extends ScalarFunction {
  def eval(value: T, broadcast: B): O = {
    fun(value, broadcast)
  }
}

class BroadcastSingleElementFilterFunction[T, B](
                                                          fun: (T, B) => Boolean)
  extends ScalarFunction {
  def eval(value: T, broadcast: B): Boolean = {
    fun(value, broadcast)
  }
}

class RandomLongFunction extends ScalarFunction {

  var r: Random = _

  override def open(context: FunctionContext): Unit = {
    super.open(context)
    r = new Random
  }

  override def isDeterministic: Boolean = false

  def eval(): Long = r.nextLong()
}

class UUIDGenerateFunction[ID: TypeInformation, DATA: TypeInformation] extends ScalarFunction {
  def eval(data: DATA): (ID, DATA) = (Generators.timeBasedGenerator().generate().toString
    .asInstanceOf[ID],
    data)

  override def isDeterministic: Boolean = false
  def getTypeInfo[K: TypeInformation]: TypeInformation[K] = implicitly[TypeInformation[K]]

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    getTypeInfo[(ID, DATA)]
}


class OneFunction extends ScalarFunction {
  def eval(): Int = {
    1
  }

  override def isDeterministic: Boolean = false
}
