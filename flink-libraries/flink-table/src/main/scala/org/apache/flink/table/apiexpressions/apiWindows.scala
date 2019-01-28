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

package org.apache.flink.table.apiexpressions

import org.apache.flink.table.api.UnresolvedOverWindow

case class ApiPartitionedOver(partitionBy: Array[ApiExpression]) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables call [[orderBy 'rowtime or orderBy 'proctime]] to specify time mode.
    *
    * For batch tables, refer to a timestamp or long attribute.
    */
  def orderBy(orderBy: ApiExpression): ApiOverWindowWithOrderBy = {
    ApiOverWindowWithOrderBy(partitionBy, orderBy)
  }
}


case class ApiOverWindowWithOrderBy(partitionBy: Seq[ApiExpression], orderBy: ApiExpression) {

  /**
    * Set the preceding offset (based on time or row-count intervals) for over window.
    *
    * @param preceding preceding offset relative to the current row.
    * @return this over window
    */
  def preceding(preceding: ApiExpression): ApiOverWindowWithPreceding = {
    new ApiOverWindowWithPreceding(partitionBy, orderBy, preceding)
  }

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: ApiExpression): ApiOverWindow = {
    ApiOverWindow(alias, partitionBy, orderBy, UNBOUNDED_RANGE, CURRENT_RANGE)
  }
}

/**
  * Over window is similar to the traditional OVER SQL.
  */
case class ApiOverWindow(
    private[flink] val alias: ApiExpression,
    private[flink] val partitionBy: Seq[ApiExpression],
    private[flink] val orderBy: ApiExpression,
    private[flink] val preceding: ApiExpression,
    private[flink] val following: ApiExpression) extends UnresolvedOverWindow

case class ApiCurrentRow() extends ApiLeafExpression

case class ApiCurrentRange() extends ApiLeafExpression

case class ApiUnboundedRow() extends ApiLeafExpression

case class ApiUnboundedRange() extends ApiLeafExpression

/**
  * A partially defined over window.
  */
class ApiOverWindowWithPreceding(
    private val partitionBy: Seq[ApiExpression],
    private val orderBy: ApiExpression,
    private val preceding: ApiExpression) {

  private[flink] var following: ApiExpression = _

  /**
    * Assigns an alias for this window that the following `select()` clause can refer to.
    *
    * @param alias alias for this over window
    * @return over window
    */
  def as(alias: ApiExpression): ApiOverWindow = {
    ApiOverWindow(alias, partitionBy, orderBy, preceding, following)
  }

  /**
    * Set the following offset (based on time or row-count intervals) for over window.
    *
    * @param following following offset that relative to the current row.
    * @return this over window
    */
  def following(following: ApiExpression): ApiOverWindowWithPreceding = {
    this.following = following
    this
  }
}

abstract class ApiWindow

// ------------------------------------------------------------------------------------------------
// Tumbling windows
// ------------------------------------------------------------------------------------------------

/**
  * Tumbling window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiTumbleWithSize(size: ApiExpression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: ApiExpression): ApiTumbleWithSizeOnTime =
    new ApiTumbleWithSizeOnTime(timeField, size)
}

/**
  * Tumbling window on time.
  */
class ApiTumbleWithSizeOnTime(time: ApiExpression, size: ApiExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: ApiExpression): ApiTumbleWithSizeOnTimeWithAlias = {
    new ApiTumbleWithSizeOnTimeWithAlias(alias, time, size)
  }
}

/**
  * Tumbling window on time with alias. Fully specifies a window.
  */
case class ApiTumbleWithSizeOnTimeWithAlias(
    alias: ApiExpression,
    timeField: ApiExpression,
    size: ApiExpression) extends ApiWindow

// ------------------------------------------------------------------------------------------------
// Sliding windows
// ------------------------------------------------------------------------------------------------


/**
  * Partially specified sliding window.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiSlideWithSize(size: ApiExpression) {

  /**
    * Specifies the window's slide as time or row-count interval.
    *
    * The slide determines the interval in which windows are started. Hence, sliding windows can
    * overlap if the slide is smaller than the size of the window.
    *
    * For example, you could have windows of size 15 minutes that slide by 3 minutes. With this
    * 15 minutes worth of elements are grouped every 3 minutes and each row contributes to 5
    * windows.
    *
    * @param slide the slide of the window either as time or row-count interval.
    * @return a sliding window
    */
  def every(slide: ApiExpression): ApiSlideWithSizeAndSlide =
    new ApiSlideWithSizeAndSlide(size, slide)
}

/**
  * Sliding window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param size the size of the window either as time or row-count interval.
  */
class ApiSlideWithSizeAndSlide(size: ApiExpression, slide: ApiExpression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: ApiExpression): ApiSlideWithSizeAndSlideOnTime =
    new ApiSlideWithSizeAndSlideOnTime(timeField, size, slide)
}

/**
  * Sliding window on time.
  */
class ApiSlideWithSizeAndSlideOnTime(
    timeField: ApiExpression,
    size: ApiExpression,
    slide: ApiExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: ApiExpression): ApiSlideWithSizeAndSlideOnTimeWithAlias = {
    new ApiSlideWithSizeAndSlideOnTimeWithAlias(alias, timeField, size, slide)
  }
}

/**
  * Sliding window on time with alias. Fully specifies a window.
  */
case class ApiSlideWithSizeAndSlideOnTimeWithAlias(
    alias: ApiExpression,
    timeField: ApiExpression,
    size: ApiExpression,
    slide: ApiExpression) extends ApiWindow

// ------------------------------------------------------------------------------------------------
// Session windows
// ------------------------------------------------------------------------------------------------


/**
  * Session window.
  *
  * For streaming tables you can specify grouping by a event-time or processing-time attribute.
  *
  * For batch tables you can specify grouping on a timestamp or long attribute.
  *
  * @param gap the time interval of inactivity before a window is closed.
  */
class ApiSessionWithGap(gap: ApiExpression) {

  /**
    * Specifies the time attribute on which rows are grouped.
    *
    * For streaming tables you can specify grouping by a event-time or processing-time attribute.
    *
    * For batch tables you can specify grouping on a timestamp or long attribute.
    *
    * @param timeField time attribute for streaming and batch tables
    * @return a tumbling window on event-time
    */
  def on(timeField: ApiExpression): ApiSessionWithGapOnTime =
    new ApiSessionWithGapOnTime(timeField, gap)
}

/**
  * Session window on time.
  */
class ApiSessionWithGapOnTime(timeField: ApiExpression, gap: ApiExpression) {

  /**
    * Assigns an alias for this window that the following `groupBy()` and `select()` clause can
    * refer to. `select()` statement can access window properties such as window start or end time.
    *
    * @param alias alias for this window
    * @return this window
    */
  def as(alias: ApiExpression): ApiSessionWithGapOnTimeWithAlias = {
    new ApiSessionWithGapOnTimeWithAlias(alias, timeField, gap)
  }
}

/**
  * Session window on time with alias. Fully specifies a window.
  */
case class ApiSessionWithGapOnTimeWithAlias(
    alias: ApiExpression,
    timeField: ApiExpression,
    gap: ApiExpression) extends ApiWindow
