package org.apache.flink.table.api.planner.converters.rex.time;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.planner.visitor.ExpressionVisitorImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.expressions.TemporalOverlaps;
import org.apache.flink.table.typeutils.TypeCheckUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * TemporalOverlapsConverter.
 */
public class TemporalOverlapsConverter {
	public static RexNode toRexNode(TemporalOverlaps temporalOverlaps, ExpressionVisitorImpl visitor) {
		return convertOverlaps(
			temporalOverlaps,
			visitor.toRexNode(temporalOverlaps.leftTimePoint()),
			visitor.toRexNode(temporalOverlaps.leftTemporal()),
			visitor.toRexNode(temporalOverlaps.rightTimePoint()),
			visitor.toRexNode(temporalOverlaps.rightTemporal()),
			(FlinkRelBuilder) visitor.getRelBuilder());
	}

	/**
	 * Standard conversion of the OVERLAPS operator.
	 * Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertOverlaps()]]
	 */
	private static RexNode convertOverlaps(
		TemporalOverlaps temporalOverlaps,
		RexNode leftP,
		RexNode leftT,
		RexNode rightP,
		RexNode rightT,
		FlinkRelBuilder relBuilder) {
		RexNode convLeftT =
			convertOverlapsEnd(relBuilder, leftP, leftT, temporalOverlaps.leftTemporal().resultType());
		RexNode convRightT =
			convertOverlapsEnd(relBuilder, rightP, rightT, temporalOverlaps.rightTemporal().resultType());

		// sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
		Tuple2<RexNode, RexNode> s0e0 = buildSwap(relBuilder, leftP, convLeftT);
		Tuple2<RexNode, RexNode> s1e1 = buildSwap(relBuilder, rightP, convRightT);

		// (e0 >= s1) AND (e1 >= s0)
		RexNode leftPred = relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, s0e0.f1, s1e1.f0);
		RexNode rightPred = relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, s1e1.f1, s0e0.f0);
		return relBuilder.call(SqlStdOperatorTable.AND, leftPred, rightPred);
	}

	private static RexNode convertOverlapsEnd(
		FlinkRelBuilder relBuilder,
		RexNode start,
		RexNode end,
		TypeInformation endType) {
		if (TypeCheckUtils.isTimeInterval(endType)) {
			return relBuilder.call(SqlStdOperatorTable.DATETIME_PLUS, start, end);
		} else {
			return end;
		}
	}

	private static Tuple2<RexNode, RexNode> buildSwap(
		FlinkRelBuilder relBuilder,
		RexNode start,
		RexNode end) {
		RexNode le = relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, start, end);
		RexNode l = relBuilder.call(SqlStdOperatorTable.CASE, le, start, end);
		RexNode r = relBuilder.call(SqlStdOperatorTable.CASE, le, end, start);
		return new Tuple2<>(l, r);
	}
}
