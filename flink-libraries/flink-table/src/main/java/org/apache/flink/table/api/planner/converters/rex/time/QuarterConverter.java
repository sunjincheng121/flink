package org.apache.flink.table.api.planner.converters.rex.time;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.expressions.Div;
import org.apache.flink.table.expressions.Extract;
import org.apache.flink.table.expressions.Literal;
import org.apache.flink.table.expressions.Minus;
import org.apache.flink.table.expressions.Plus;
import org.apache.flink.table.expressions.Quarter;
import org.apache.flink.table.expressions.SymbolExpression;
import org.apache.flink.table.expressions.TimeIntervalUnit;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexNode;

/**
 * QuarterConverter.
 */
public class QuarterConverter {

	/**
	 * Standard conversion of the QUARTER operator.
	 * Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertQuarter()]]
	 */
	public static RexNode toRexNode(Quarter quarter, ExpressionVisitorImpl visitor) {
		return visitor.toRexNode(Plus.apply(
			Div.apply(
				Minus.apply(
					Extract.apply(SymbolExpression.apply(TimeIntervalUnit.MONTH()), quarter.child()),
					Literal.apply(1L, BasicTypeInfo.LONG_TYPE_INFO)),
				Literal.apply(TimeUnit.QUARTER.multiplier.longValue(), BasicTypeInfo.LONG_TYPE_INFO)),
			Literal.apply(1L, BasicTypeInfo.LONG_TYPE_INFO)
		));
	}
}
