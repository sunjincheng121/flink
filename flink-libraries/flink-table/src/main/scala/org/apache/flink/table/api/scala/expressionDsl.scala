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
package org.apache.flink.table.api.scala

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.calcite.avatica.util.DateTimeUtils._
import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api._
import org.apache.flink.table.apiexpressions.ApiTimeIntervalUnit.ApiTimeIntervalUnit
import org.apache.flink.table.apiexpressions.ApiTimePointUnit.ApiTimePointUnit
import org.apache.flink.table.apiexpressions.{ApiAlias, ApiCall, ApiCast, ApiCurrentRange, ApiCurrentRow, ApiDistinctAgg, ApiDistinctAggExpression, ApiExpression, ApiExpressionUtils, ApiFlattening, ApiGetCompositeField, ApiIn, ApiLiteral, ApiProctimeAttribute, ApiRowtimeAttribute, ApiScalarFunctionCall, ApiTableReference, ApiTimePointUnit, ApiTrimConstants, ApiTrimMode, ApiUDAGGExpression, ApiUnboundedRange, ApiUnboundedRow, ApiUnresolvedFieldReference, ApiUnresolvedOverCall}
import org.apache.flink.table.functions.{AggregateFunction, DistinctAggregateFunction, ScalarFunction}

import _root_.scala.language.implicitConversions

/**
  * These are all the operations that can be used to construct an [[ApiExpression]] AST for
  * ApiExpression operations.
  *
  * These operations must be kept in sync with the parser in
  * [[org.apache.flink.table.expressions.ExpressionParser]].
  */
trait ApiImplicitExpressionOperations {
  private[flink] def expr: ApiExpression

  /**
    * Enables literals on left side of binary ApiExpressions.
    *
    * e.g. 12.toExpr % 'a
    *
    * @return ApiExpression
    */
  def toExpr: ApiExpression = expr

  /**
    * Boolean AND in three-valued logic.
    */
  def && (other: ApiExpression) = ApiCall("and", Seq(expr, other))

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: ApiExpression) = ApiCall("or", Seq(expr, other))

  /**
    * Greater than.
    */
  def > (other: ApiExpression) = ApiCall("greaterThan", Seq(expr, other))

  /**
    * Greater than or equal.
    */
  def >= (other: ApiExpression) = ApiCall("greaterThanOrEqual", Seq(expr, other))

  /**
    * Less than.
    */
  def < (other: ApiExpression) = ApiCall("lessThan", Seq(expr, other))

  /**
    * Less than or equal.
    */
  def <= (other: ApiExpression) = ApiCall("lessThanOrEqual", Seq(expr, other))

  /**
    * Equals.
    */
  def === (other: ApiExpression) = ApiCall("equals", Seq(expr, other))

  /**
    * Not equal.
    */
  def !== (other: ApiExpression) = ApiCall("notEquals", Seq(expr, other))

  /**
    * Whether boolean ApiExpression is not true; returns null if boolean is null.
    */
  def unary_! = ApiCall("not", Seq(expr))

  /**
    * Returns negative numeric.
    */
  def unary_- = ApiCall("minusPrefix", Seq(expr))

  /**
    * Returns numeric.
    */
  def unary_+ : ApiExpression = expr

  /**
    * Returns true if the given ApiExpression is null.
    */
  def isNull = ApiCall("isNull", Seq(expr))

  /**
    * Returns true if the given ApiExpression is not null.
    */
  def isNotNull = ApiCall("isNotNull", Seq(expr))

  /**
    * Returns true if given boolean ApiExpression is true. False otherwise (for null and false).
    */
  def isTrue = ApiCall("isTrue", Seq(expr))

  /**
    * Returns true if given boolean ApiExpression is false. False otherwise (for null and true).
    */
  def isFalse = ApiCall("isFalse", Seq(expr))

  /**
    * Returns true if given boolean ApiExpression is not true (for null and false). False otherwise.
    */
  def isNotTrue = ApiCall("isNotTrue", Seq(expr))

  /**
    * Returns true if given boolean ApiExpression is not false (for null and true). False otherwise.
    */
  def isNotFalse = ApiCall("isNotFalse", Seq(expr))

  /**
    * Returns left plus right.
    */
  def + (other: ApiExpression) = ApiCall("plus", Seq(expr, other))

  /**
    * Returns left minus right.
    */
  def - (other: ApiExpression) = ApiCall("minus", Seq(expr, other))

  /**
    * Returns left divided by right.
    */
  def / (other: ApiExpression) = ApiCall("divide", Seq(expr, other))

  /**
    * Returns left multiplied by right.
    */
  def * (other: ApiExpression) = ApiCall("times", Seq(expr, other))

  /**
    * Returns the remainder (modulus) of left divided by right.
    * The result is negative only if left is negative.
    */
  def % (other: ApiExpression) = mod(other)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, null is returned.
    */
  def sum = ApiCall("sum", Seq(expr))

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, 0 is returned.
    */
  def sum0 = ApiCall("sum0", Seq(expr))

  /**
    * Returns the minimum value of field across all input values.
    */
  def min =ApiCall("min", Seq(expr))

  /**
    * Returns the maximum value of field across all input values.
    */
  def max = ApiCall("max", Seq(expr))

  /**
    * Returns the number of input rows for which the field is not null.
    */
  def count = ApiCall("count", Seq(expr))

  /**
    * Returns the average (arithmetic mean) of the numeric field across all input values.
    */
  def avg = ApiCall("avg", Seq(expr))

  /**
    * Returns the population standard deviation of an ApiExpression (the square root of varPop()).
    */
  def stddevPop = ApiCall("stddevPop", Seq(expr))

  /**
    * Returns the sample standard deviation of an ApiExpression (the square root of varSamp()).
    */
  def stddevSamp = ApiCall("stddevSamp", Seq(expr))

  /**
    * Returns the population standard variance of an ApiExpression.
    */
  def varPop = ApiCall("varPop", Seq(expr))

  /**
    *  Returns the sample variance of a given ApiExpression.
    */
  def varSamp = ApiCall("varSamp", Seq(expr))

  /**
    * Returns multiset aggregate of a given ApiExpression.
    */
  def collect = ApiCall("collect", Seq(expr))

  /**
    * Converts a value to a given type.
    *
    * e.g. "42".cast(Types.INT) leads to 42.
    *
    * @return casted ApiExpression
    */
  def cast(toType: TypeInformation[_]) = ApiCast(expr, toType)

  /**
    * Specifies a name for an ApiExpression i.e. a field.
    *
    * @param name name for one field
    * @param extraNames additional names if the ApiExpression expands to multiple fields
    * @return field with an alias
    */
  def as(name: Symbol, extraNames: Symbol*) = ApiAlias(expr, name.name, extraNames.map(_.name))

  /**
    * Specifies ascending order of an ApiExpression i.e. a field for orderBy call.
    *
    * @return ascend ApiExpression
    */
  def asc = ApiCall("asc", Seq(expr))

  /**
    * Specifies descending order of an ApiExpression i.e. a field for orderBy call.
    *
    * @return descend ApiExpression
    */
  def desc = ApiCall("desc", Seq(expr))

  /**
    * Returns true if an ApiExpression exists in a given list of ApiExpressions. This is a shorthand
    * for multiple OR conditions.
    *
    * If the testing set contains null, the result will be null if the element can not be found
    * and true if it can be found. If the element is null, the result is always null.
    *
    * e.g. "42".in(1, 2, 3) leads to false.
    */
  def in(elements: ApiExpression*) = ApiIn(expr, elements)

  /**
    * Returns true if an ApiExpression exists in a given table sub-query. The sub-query table
    * must consist of one column. This column must have the same data type as the ApiExpression.
    *
    * Note: This operation is not supported in a streaming environment yet.
    */
  def in(table: Table) = ApiIn(expr, Seq(ApiTableReference(table.toString, table)))

  /**
    * Returns the start time (inclusive) of a window when applied on a window reference.
    */
  def start = ApiCall("start", Seq(expr))

  /**
    * Returns the end time (exclusive) of a window when applied on a window reference.
    *
    * e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
    */
  def end = ApiCall("end", Seq(expr))

  /**
    * Ternary conditional operator that decides which of two other ApiExpressions should be
    * evaluated based on a evaluated boolean condition.
    *
    * e.g. (42 > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue ApiExpression to be evaluated if condition holds
    * @param ifFalse ApiExpression to be evaluated if condition does not hold
    */
  def ?(ifTrue: ApiExpression, ifFalse: ApiExpression) = {
    ApiCall("if", Seq(expr, ifTrue, ifFalse))
  }

  // scalar functions

  /**
    * Calculates the remainder of division the given number by another one.
    */
  def mod(other: ApiExpression) = ApiCall("mod", Seq(expr, other))

  /**
    * Calculates the Euler's number raised to the given power.
    */
  def exp() = ApiCall("exp", Seq(expr))

  /**
    * Calculates the base 10 logarithm of the given value.
    */
  def log10() = ApiCall("log10", Seq(expr))

  /**
    * Calculates the base 2 logarithm of the given value.
    */
  def log2() = ApiCall("log2", Seq(expr))

  /**
    * Calculates the natural logarithm of the given value.
    */
  def ln() = ApiCall("ln", Seq(expr))

  /**
    * Calculates the natural logarithm of the given value.
    */
  def log() = ApiCall("log", Seq(expr))

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: ApiExpression) = ApiCall("log", Seq(base, expr))

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: ApiExpression) = ApiCall("power", Seq(expr, other))

  /**
    * Calculates the hyperbolic cosine of a given value.
    */
  def cosh() = ApiCall("cosh", Seq(expr))

  /**
    * Calculates the square root of a given value.
    */
  def sqrt() = ApiCall("sqrt", Seq(expr))

  /**
    * Calculates the absolute value of given value.
    */
  def abs() = ApiCall("abs", Seq(expr))

  /**
    * Calculates the largest integer less than or equal to a given number.
    */
  def floor() = ApiCall("floor", Seq(expr))

  /**
    * Calculates the hyperbolic sine of a given value.
    */
  def sinh() = ApiCall("sinh", Seq(expr))

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil() = ApiCall("ceil", Seq(expr))

  /**
    * Calculates the sine of a given number.
    */
  def sin() = ApiCall("sin", Seq(expr))

  /**
    * Calculates the cosine of a given number.
    */
  def cos() = ApiCall("cos", Seq(expr))

  /**
    * Calculates the tangent of a given number.
    */
  def tan() = ApiCall("tan", Seq(expr))

  /**
    * Calculates the cotangent of a given number.
    */
  def cot() = ApiCall("cot", Seq(expr))

  /**
    * Calculates the arc sine of a given number.
    */
  def asin() = ApiCall("asin", Seq(expr))

  /**
    * Calculates the arc cosine of a given number.
    */
  def acos() = ApiCall("acos", Seq(expr))

  /**
    * Calculates the arc tangent of a given number.
    */
  def atan() = ApiCall("atan", Seq(expr))

  /**
    * Calculates the hyperbolic tangent of a given number.
    */
  def tanh() = ApiCall("tanh", Seq(expr))

  /**
    * Converts numeric from radians to degrees.
    */
  def degrees() = ApiCall("degrees", Seq(expr))

  /**
    * Converts numeric from degrees to radians.
    */
  def radians() = ApiCall("radians", Seq(expr))

  /**
    * Calculates the signum of a given number.
    */
  def sign() = ApiCall("sign", Seq(expr))

  /**
    * Rounds the given number to integer places right to the decimal point.
    */
  def round(places: ApiExpression) = ApiCall("round", Seq(expr, places))

  /**
    * Returns a string representation of an integer numeric value in binary format. Returns null if
    * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
    */
  def bin() = ApiCall("bin", Seq(expr))

  /**
    * Returns a string representation of an integer numeric value or a string in hex format. Returns
    * null if numeric or string is null.
    *
    * E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
    * to "68656c6c6f2c776f726c64".
    */
  def hex() = ApiCall("hex", Seq(expr))

  // String operations

  /**
    * Creates a substring of the given string at given index for a given length.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param length number of characters of the substring
    * @return substring
    */
  def substring(beginIndex: ApiExpression, length: ApiExpression) =
    ApiCall("substring", Seq(expr, beginIndex, length))

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: ApiExpression) =
    ApiCall("substring", Seq(expr, beginIndex))

  /**
    * Removes leading and/or trailing characters from the given string.
    *
    * @param removeLeading if true, remove leading characters (default: true)
    * @param removeTrailing if true, remove trailing characters (default: true)
    * @param character string containing the character (default: " ")
    * @return trimmed string
    */
  def trim(
      removeLeading: Boolean = true,
      removeTrailing: Boolean = true,
      character: ApiExpression = ApiTrimConstants.TRIM_DEFAULT_CHAR) = {
    if (removeLeading && removeTrailing) {
      ApiCall("trim", Seq(ApiTrimMode.BOTH, character, expr))
    } else if (removeLeading) {
      ApiCall("trim", Seq(ApiTrimMode.LEADING, character, expr))
    } else if (removeTrailing) {
      ApiCall("trim", Seq(ApiTrimMode.TRAILING, character, expr))
    } else {
      expr
    }
  }

  /**
    * Returns a new string which replaces all the occurrences of the search target
    * with the replacement string (non-overlapping).
    */
  def replace(search: ApiExpression, replacement: ApiExpression) =
    ApiCall("replace", Seq(expr, search, replacement))

  /**
    * Returns the length of a string.
    */
  def charLength() = ApiCall("charLength", Seq(expr))

  /**
    * Returns all of the characters in a string in upper case using the rules of
    * the default locale.
    */
  def upperCase() = ApiCall("upper", Seq(expr))

  /**
    * Returns all of the characters in a string in lower case using the rules of
    * the default locale.
    */
  def lowerCase() = ApiCall("lower", Seq(expr))

  /**
    * Converts the initial letter of each word in a string to uppercase.
    * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap() = ApiCall("initCap", Seq(expr))

  /**
    * Returns true, if a string matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: ApiExpression) = ApiCall("like", Seq(expr, pattern))

  /**
    * Returns true, if a string matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all strings that consist of at least one A
    */
  def similar(pattern: ApiExpression) = ApiCall("similar", Seq(expr, pattern))

  /**
    * Returns the position of string in an other string starting at 1.
    * Returns 0 if string could not be found.
    *
    * e.g. "a".position("bbbbba") leads to 6
    */
  def position(haystack: ApiExpression) = ApiCall("position", Seq(expr, haystack))

  /**
    * Returns a string left-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".lpad(4, '??') returns "??hi",  "hi".lpad(1, '??') returns "h"
    */
  def lpad(len: ApiExpression, pad: ApiExpression) = ApiCall("lpad", Seq(expr, len, pad))

  /**
    * Returns a string right-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".rpad(4, '??') returns "hi??",  "hi".rpad(1, '??') returns "h"
    */
  def rpad(len: ApiExpression, pad: ApiExpression) = ApiCall("rpad", Seq(expr, len, pad))

  /**
    * For windowing function to config over window
    * e.g.:
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    */
  def over(alias: ApiExpression): ApiExpression = {
     ApiUnresolvedOverCall(expr, alias)
  }

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6) leads to "xxxxxxxxx"
    */
  def overlay(newString: ApiExpression, starting: ApiExpression) =
    ApiCall("overlay", Seq(expr, newString, starting))

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    * The length specifies how many characters should be removed.
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
    */
  def overlay(newString: ApiExpression, starting: ApiExpression, length: ApiExpression) =
    ApiCall("overlay", Seq(expr, newString, starting, length))

  /**
    * Returns a string with all substrings that match the regular ApiExpression consecutively
    * being replaced.
    */
  def regexpReplace(regex: ApiExpression, replacement: ApiExpression) =
    ApiCall("regexpReplace", Seq(expr, regex, replacement))

  /**
    * Returns a string extracted with a specified regular ApiExpression and a regex match group
    * index.
    */
  def regexpExtract(regex: ApiExpression, extractIndex: ApiExpression) =
    ApiCall("regexpExtract", Seq(expr, regex, extractIndex))

  /**
    * Returns a string extracted with a specified regular ApiExpression.
    */
  def regexpExtract(regex: ApiExpression) =
    ApiCall("regexpExtract", Seq(expr, regex))

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64() = ApiCall("fromBase64", Seq(expr))

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64() = ApiCall("toBase64", Seq(expr))

  /**
    * Returns a string that removes the left whitespaces from the given string.
    */
  def ltrim() = ApiCall("ltrim", Seq(expr))

  /**
    * Returns a string that removes the right whitespaces from the given string.
    */
  def rtrim() = ApiCall("rtrim", Seq(expr))

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(n: ApiExpression) = ApiCall("repeat", Seq(expr, n))

  // Temporal operations

  /**
    * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
    */
  def toDate = ApiCast(expr, SqlTimeTypeInfo.DATE)

  /**
    * Parses a time string in the form "HH:mm:ss" to a SQL Time.
    */
  def toTime = ApiCast(expr, SqlTimeTypeInfo.TIME)

  /**
    * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
    */
  def toTimestamp = ApiCast(expr, SqlTimeTypeInfo.TIMESTAMP)

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: ApiTimeIntervalUnit) =
    ApiCall("extract", Seq(timeIntervalUnit, expr))

  /**
    * Rounds down a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.floor(MINUTE) leads to 12:44:00
    */
  def floor(timeIntervalUnit: ApiTimeIntervalUnit) =
    ApiCall("temporalFloor", Seq(timeIntervalUnit, expr))

  /**
    * Rounds up a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.ceil(MINUTE) leads to 12:45:00
    */
  def ceil(timeIntervalUnit: ApiTimeIntervalUnit) =
    ApiCall("temporalCeil", Seq(timeIntervalUnit, expr))

  // Interval types

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def year: ApiExpression = ApiExpressionUtils.toMonthInterval(expr, 12)

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def years: ApiExpression = year

  /**
    * Creates an interval of the given number of quarters.
    *
    * @return interval of months
    */
  def quarter: ApiExpression = ApiExpressionUtils.toMonthInterval(expr, 3)

  /**
    * Creates an interval of the given number of quarters.
    *
    * @return interval of months
    */
  def quarters: ApiExpression = quarter

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def month: ApiExpression = ApiExpressionUtils.toMonthInterval(expr, 1)

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def months: ApiExpression = month

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def week: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, 7 * MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def weeks: ApiExpression = week

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def day: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def days: ApiExpression = day

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hour: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, MILLIS_PER_HOUR)

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hours: ApiExpression = hour

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minute: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, MILLIS_PER_MINUTE)

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minutes: ApiExpression = minute

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def second: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, MILLIS_PER_SECOND)

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def seconds: ApiExpression = second

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def milli: ApiExpression = ApiExpressionUtils.toMilliInterval(expr, 1)

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def millis: ApiExpression = milli

  // Row interval type

  /**
    * Creates an interval of rows.
    *
    * @return interval of rows
    */
  def rows: ApiExpression = ApiExpressionUtils.toRowInterval(expr)

  // Advanced type helper functions

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and
    * returns it's value.
    *
    * @param name name of the field (similar to Flink's field ApiExpressions)
    * @return value of the field
    */
  def get(name: String) = ApiGetCompositeField(expr, name)

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
    * returns it's value.
    *
    * @param index position of the field
    * @return value of the field
    */
  def get(index: Int) = ApiGetCompositeField(expr, index)

  /**
    * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
    * into a flat representation where every subtype is a separate field.
    */
  def flatten() = ApiFlattening(expr)

  /**
    * Accesses the element of an array or map based on a key or an index (starting at 1).
    *
    * @param index key or position of the element (array index starting at 1)
    * @return value of the element
    */
  def at(index: ApiExpression) = ApiCall("at", Seq(expr, index))

  /**
    * Returns the number of elements of an array or number of entries of a map.
    *
    * @return number of elements or entries
    */
  def cardinality() = ApiCall("cardinality", Seq(expr))

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element() = ApiCall("element", Seq(expr))

  // Time definition

  /**
    * Declares a field as the rowtime attribute for indicating, accessing, and working in
    * Flink's event time.
    */
  def rowtime = ApiRowtimeAttribute(expr)

  /**
    * Declares a field as the proctime attribute for indicating, accessing, and working in
    * Flink's processing time.
    */
  def proctime = ApiProctimeAttribute(expr)

  // Hash functions

  /**
    * Returns the MD5 hash of the string argument; null if string is null.
    *
    * @return string of 32 hexadecimal digits or null
    */
  def md5() = ApiCall("md5", Seq(expr))

  /**
    * Returns the SHA-1 hash of the string argument; null if string is null.
    *
    * @return string of 40 hexadecimal digits or null
    */
  def sha1() = ApiCall("sha1", Seq(expr))

  /**
    * Returns the SHA-224 hash of the string argument; null if string is null.
    *
    * @return string of 56 hexadecimal digits or null
    */
  def sha224() = ApiCall("sha224", Seq(expr))

  /**
    * Returns the SHA-256 hash of the string argument; null if string is null.
    *
    * @return string of 64 hexadecimal digits or null
    */
  def sha256() = ApiCall("sha256", Seq(expr))

  /**
    * Returns the SHA-384 hash of the string argument; null if string is null.
    *
    * @return string of 96 hexadecimal digits or null
    */
  def sha384() = ApiCall("sha384", Seq(expr))

  /**
    * Returns the SHA-512 hash of the string argument; null if string is null.
    *
    * @return string of 128 hexadecimal digits or null
    */
  def sha512() = ApiCall("sha512", Seq(expr))

  /**
    * Returns the hash for the given string ApiExpression using the SHA-2 family of hash
    * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
    *
    * @param hashLength bit length of the result (either 224, 256, 384, or 512)
    * @return string or null if one of the arguments is null.
    */
  def sha2(hashLength: ApiExpression) = ApiCall("sha2", Seq(expr, hashLength))

  /**
    * Returns true if the given ApiExpression is between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable ApiExpression
    * @param upperBound numeric or comparable ApiExpression
    * @return boolean or null
    */
  def between(lowerBound: ApiExpression, upperBound: ApiExpression) =
    ApiCall("between", Seq(expr, lowerBound, upperBound))

  /**
    * Returns true if the given ApiExpression is not between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable ApiExpression
    * @param upperBound numeric or comparable ApiExpression
    * @return boolean or null
    */
  def notBetween(lowerBound: ApiExpression, upperBound: ApiExpression) =
    ApiCall("notBetween", Seq(expr, lowerBound, upperBound))
}

/**
  * Implicit conversions from Scala Literals to ApiExpression [[ApiLiteral]] and from
  * [[ApiExpression]] to [[ApiImplicitExpressionOperations]].
  */
trait ApiImplicitExpressionConversions {

  implicit val UNBOUNDED_ROW = ApiUnboundedRow()
  implicit val UNBOUNDED_RANGE = ApiUnboundedRange()

  implicit val CURRENT_ROW = ApiCurrentRow()
  implicit val CURRENT_RANGE = ApiCurrentRange()

  implicit class ApiWithOperations(e: ApiExpression) extends ApiImplicitExpressionOperations {
    def expr = e
  }

  implicit class ApiUnresolvedFieldExpression(s: Symbol) extends ApiImplicitExpressionOperations {
    def expr = ApiUnresolvedFieldReference(s.name)
  }

  implicit class ApiLiteralLongExpression(l: Long) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(l)
  }

  implicit class ApiLiteralByteExpression(b: Byte) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(b)
  }

  implicit class ApiLiteralShortExpression(s: Short) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(s)
  }

  implicit class ApiLiteralIntExpression(i: Int) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(i)
  }

  implicit class ApiLiteralFloatExpression(f: Float) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(f)
  }

  implicit class ApiLiteralDoubleExpression(d: Double) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(d)
  }

  implicit class ApiLiteralStringExpression(str: String) extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(str)
  }

  implicit class ApiLiteralBooleanExpression(bool: Boolean)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(bool)
  }

  implicit class ApiLiteralJavaDecimalExpression(javaDecimal: JBigDecimal)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(javaDecimal)
  }

  implicit class ApiLiteralScalaDecimalExpression(scalaDecimal: BigDecimal)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(scalaDecimal.bigDecimal)
  }

  implicit class ApiLiteralSqlDateExpression(sqlDate: Date)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(sqlDate)
  }

  implicit class ApiLiteralSqlTimeExpression(sqlTime: Time)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(sqlTime)
  }

  implicit class ApiLiteralSqlTimestampExpression(sqlTimestamp: Timestamp)
    extends ApiImplicitExpressionOperations {
    def expr = ApiLiteral(sqlTimestamp)
  }

  implicit class ApiScalarFunctionCallExpression(val s: ScalarFunction) {
    def apply(params: ApiExpression*): ApiExpression = {
      ApiScalarFunctionCall(s, params)
    }
  }

  implicit def apiSymbol2FieldExpression(sym: Symbol): ApiExpression =
    ApiUnresolvedFieldReference(sym.name)
  implicit def apiByte2Literal(b: Byte): ApiExpression = ApiLiteral(b)
  implicit def apiShort2Literal(s: Short): ApiExpression = ApiLiteral(s)
  implicit def apiInt2Literal(i: Int): ApiExpression = ApiLiteral(i)
  implicit def apiLong2Literal(l: Long): ApiExpression = ApiLiteral(l)
  implicit def apiDouble2Literal(d: Double): ApiExpression = ApiLiteral(d)
  implicit def apiFloat2Literal(d: Float): ApiExpression = ApiLiteral(d)
  implicit def apiString2Literal(str: String): ApiExpression = ApiLiteral(str)
  implicit def apiBoolean2Literal(bool: Boolean): ApiExpression = ApiLiteral(bool)
  implicit def apiJavaDec2Literal(javaDec: JBigDecimal): ApiExpression = ApiLiteral(javaDec)
  implicit def apiScalaDec2Literal(scalaDec: BigDecimal): ApiExpression =
    ApiLiteral(scalaDec.bigDecimal)
  implicit def apiSqlDate2Literal(sqlDate: Date): ApiExpression = ApiLiteral(sqlDate)
  implicit def apiSqlTime2Literal(sqlTime: Time): ApiExpression = ApiLiteral(sqlTime)
  implicit def apiSqlTimestamp2Literal(sqlTimestamp: Timestamp): ApiExpression =
    ApiLiteral(sqlTimestamp)
  implicit def apiArray2ArrayConstructor(array: Array[_]): ApiExpression =
    ApiExpressionUtils.convertArray(array)
  implicit def apiUserDefinedAggFunctionConstructor[T: TypeInformation, ACC: TypeInformation]
  (udagg: AggregateFunction[T, ACC]): ApiUDAGGExpression[T, ACC] = ApiUDAGGExpression(udagg)
  implicit def apiToDistinct(agg: ApiCall): ApiDistinctAggExpression =
    ApiDistinctAggExpression(agg)
  implicit def apiToDistinct[T: TypeInformation, ACC: TypeInformation]
  (agg: AggregateFunction[T, ACC]): DistinctAggregateFunction[T, ACC] =
    DistinctAggregateFunction(agg)

  // just for test pass
  implicit def apiToInnerTable(table: Table): InnerTable = table.asInstanceOf[InnerTable]
}

// ------------------------------------------------------------------------------------------------
// ApiExpressions with no parameters
// ------------------------------------------------------------------------------------------------

// we disable the object checker here as it checks for capital letters of objects
// but we want that objects look like functions in certain cases e.g. array(1, 2, 3)
// scalastyle:off object.name

/**
  * Returns the current SQL date in UTC time zone.
  */
object currentDate {

  /**
    * Returns the current SQL date in UTC time zone.
    */
  def apply(): ApiExpression = {
    ApiCall("currentDate", Seq())
  }
}

/**
  * Returns the current SQL time in UTC time zone.
  */
object currentTime {

  /**
    * Returns the current SQL time in UTC time zone.
    */
  def apply(): ApiExpression = {
    ApiCall("currentTime", Seq())
  }
}

/**
  * Returns the current SQL timestamp in UTC time zone.
  */
object currentTimestamp {

  /**
    * Returns the current SQL timestamp in UTC time zone.
    */
  def apply(): ApiExpression = {
    ApiCall("currentTimestamp", Seq())
  }
}

/**
  * Returns the current SQL time in local time zone.
  */
object localTime {

  /**
    * Returns the current SQL time in local time zone.
    */
  def apply(): ApiExpression = {
    ApiCall("localTime", Seq())
  }
}

/**
  * Returns the current SQL timestamp in local time zone.
  */
object localTimestamp {

  /**
    * Returns the current SQL timestamp in local time zone.
    */
  def apply(): ApiExpression = {
    ApiCall("localTimestamp", Seq())
  }
}

/**
  * Determines whether two anchored time intervals overlap. Time point and temporal are
  * transformed into a range defined by two time points (start, end). The function
  * evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>.
  *
  * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
  *
  * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
  */
object temporalOverlaps {

  /**
    * Determines whether two anchored time intervals overlap. Time point and temporal are
    * transformed into a range defined by two time points (start, end).
    *
    * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
    *
    * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
    */
  def apply(
    leftTimePoint: ApiExpression,
    leftTemporal: ApiExpression,
    rightTimePoint: ApiExpression,
    rightTemporal: ApiExpression): ApiExpression = {
    ApiCall("temporalOverlaps", Seq(leftTimePoint, leftTemporal, rightTimePoint, rightTemporal))
  }
}

/**
  * Formats a timestamp as a string using a specified format.
  * The format must be compatible with MySQL's date formatting syntax as used by the
  * date_parse function.
  *
  * For example <code>dataFormat('time, "%Y, %d %M")</code> results in strings
  * formatted as "2017, 05 May".
  */
object dateFormat {

  /**
    * Formats a timestamp as a string using a specified format.
    * The format must be compatible with MySQL's date formatting syntax as used by the
    * date_parse function.
    *
    * For example dataFormat('time, "%Y, %d %M") results in strings formatted as "2017, 05 May".
    *
    * @param timestamp The timestamp to format as string.
    * @param format The format of the string.
    * @return The formatted timestamp as string.
    */
  def apply(
    timestamp: ApiExpression,
    format: ApiExpression): ApiExpression = {
    ApiCall("dateFormat", Seq(timestamp, format))
  }
}

/**
  * Returns the (signed) number of [[ApiTimePointUnit]] between timePoint1 and timePoint2.
  *
  * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
  * to 3.
  */
object timestampDiff {

  /**
    * Returns the (signed) number of [[ApiTimePointUnit]] between timePoint1 and timePoint2.
    *
    * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
    * to 3.
    *
    * @param timePointUnit The unit to compute diff.
    * @param timePoint1 The first point in time.
    * @param timePoint2 The second point in time.
    * @return The number of intervals as integer value.
    */
  def apply(
    timePointUnit: ApiTimePointUnit,
    timePoint1: ApiExpression,
    timePoint2: ApiExpression)
  : ApiExpression = {
    ApiCall("timestampDiff", Seq(timePointUnit, timePoint1, timePoint2))
  }
}

/**
  * Creates an array of literals. The array will be an array of objects (not primitives).
  */
object array {

  /**
    * Creates an array of literals. The array will be an array of objects (not primitives).
    */
  def apply(head: ApiExpression, tail: ApiExpression*): ApiExpression = {
    ApiCall("array", head +: tail.toSeq)
  }
}

/**
  * Creates a row of ApiExpressions.
  */
object row {

  /**
    * Creates a row of ApiExpressions.
    */
  def apply(head: ApiExpression, tail: ApiExpression*): ApiExpression = {
    ApiCall("row", head +: tail.toSeq)
  }
}

/**
  * Creates a map of ApiExpressions. The map will be a map between two objects (not primitives).
  */
object map {

  /**
    * Creates a map of ApiExpressions. The map will be a map between two objects (not primitives).
    */
  def apply(key: ApiExpression, value: ApiExpression, tail: ApiExpression*): ApiExpression = {
    ApiCall("map", Seq(key, value) ++ tail.toSeq)
  }
}

/**
  * Returns a value that is closer than any other value to pi.
  */
object pi {

  /**
    * Returns a value that is closer than any other value to pi.
    */
  def apply(): ApiExpression = {
    ApiCall("pi", Seq())
  }
}

/**
  * Returns a value that is closer than any other value to e.
  */
object e {

  /**
    * Returns a value that is closer than any other value to e.
    */
  def apply(): ApiExpression = {
    ApiCall("e", Seq())
  }
}

/**
  * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
  */
object rand {

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
    */
  def apply(): ApiExpression = {
    ApiCall("rand", Seq())
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def apply(seed: ApiExpression): ApiExpression = {
    ApiCall("rand", Seq(seed))
  }
}

/**
  * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
  * value (exclusive).
  */
object randInteger {

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
    * value (exclusive).
    */
  def apply(bound: ApiExpression): ApiExpression = {
    ApiCall("randInteger", Seq(bound))
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def apply(seed: ApiExpression, bound: ApiExpression): ApiExpression = {
    ApiCall("randInteger", Seq(seed, bound))
  }
}

/**
  * Returns the string that results from concatenating the arguments.
  * Returns NULL if any argument is NULL.
  */
object concat {

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  def apply(string: ApiExpression, strings: ApiExpression*): ApiExpression = {
    ApiCall("concat", Seq(string) ++ strings)
  }
}

/**
  * Calculates the arc tangent of a given coordinate.
  */
object atan2 {

  /**
    * Calculates the arc tangent of a given coordinate.
    */
  def apply(y: ApiExpression, x: ApiExpression): ApiExpression = {
    ApiCall("atan2", Seq(y, x))
  }
}

/**
  * Returns the string that results from concatenating the arguments and separator.
  * Returns NULL If the separator is NULL.
  *
  * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
  * values after the separator argument.
  **/
object concat_ws {
  def apply(separator: ApiExpression, string: ApiExpression, strings: ApiExpression*)
  : ApiExpression = {
    ApiCall("concat_ws", Seq(separator) ++ Seq(string) ++ strings)
  }
}

/**
  * Returns an UUID (Universally Unique Identifier) string (e.g.,
  * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
  * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
  * generator.
  */
object uuid {

  /**
    * Returns an UUID (Universally Unique Identifier) string (e.g.,
    * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    * generator.
    */
  def apply(): ApiExpression = {
    ApiCall("uuid", Seq())
  }
}

// scalastyle:on object.name
