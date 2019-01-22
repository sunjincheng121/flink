/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.planner.converters.rex.literals;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

/**
 * LiteralConverter.
 */
public class LiteralConverter {
	public static RexNode toRexNode(Literal literal, ExpressionVisitorImpl visitor) {
		TypeInformation type = literal.resultType();
		if (type.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {

			BigDecimal bigDecValue = (BigDecimal) literal.value();
			RelDataType decType = visitor.getRelBuilder().getTypeFactory().createSqlType(
				SqlTypeName.DECIMAL);
			return visitor.getRelBuilder().getRexBuilder().makeExactLiteral(bigDecValue, decType);

		} else if (type.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
			// create BIGINT literals for long type
			BigDecimal bigint = BigDecimal.valueOf((Long) literal.value());
			return visitor.getRelBuilder().getRexBuilder().makeBigintLiteral(bigint);

		} else if (type.equals(SqlTimeTypeInfo.DATE)) {
			// date
			DateString datestr = DateString.fromCalendarFields(valueAsCalendar(literal.value()));
			return visitor.getRelBuilder().getRexBuilder().makeDateLiteral(datestr);

		} else if (type.equals(SqlTimeTypeInfo.TIME)) {
			//time
			TimeString timestr = TimeString.fromCalendarFields(valueAsCalendar(literal.value()));
			return visitor.getRelBuilder().getRexBuilder().makeTimeLiteral(timestr, 0);

		} else if (type.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			//timestamp
			TimestampString timestampstr =
				TimestampString.fromCalendarFields(valueAsCalendar(literal.value()));
			return visitor.getRelBuilder().getRexBuilder().makeTimestampLiteral(timestampstr, 3);

		} else if (type.equals(TimeIntervalTypeInfo.INTERVAL_MONTHS())) {

			BigDecimal interval = BigDecimal.valueOf((Integer) literal.value());
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
				TimeUnit.YEAR,
				TimeUnit.MONTH,
				SqlParserPos.ZERO);
			return visitor.getRelBuilder().getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);

		} else if (type.equals(TimeIntervalTypeInfo.INTERVAL_MILLIS())) {

			BigDecimal interval = BigDecimal.valueOf((Long) literal.value());
			SqlIntervalQualifier intervalQualifier = new SqlIntervalQualifier(
				TimeUnit.DAY,
				TimeUnit.SECOND,
				SqlParserPos.ZERO);
			return visitor.getRelBuilder().getRexBuilder().makeIntervalLiteral(interval, intervalQualifier);

		} else {

			return visitor.getRelBuilder().literal(literal.value());

		}
	}

	private static Calendar valueAsCalendar(Object date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime((Date) date);
		return calendar;
	}
}
