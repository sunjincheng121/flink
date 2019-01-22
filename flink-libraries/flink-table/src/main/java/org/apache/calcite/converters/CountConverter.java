package org.apache.calcite.converters;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.visitor.RexNodeVisitorImpl;
import org.apache.flink.table.expressions.Count;

public class CountConverter {
    public static RelBuilder.AggCall toAggCall(Count agg, String name, boolean isDistinct, RelBuilder relBuilder) {

        return relBuilder.aggregateCall(
                SqlStdOperatorTable.COUNT,
                isDistinct,
                false,
                null,
                name,
                agg.child().accept(new RexNodeVisitorImpl(relBuilder)));
    }

    public static SqlAggFunction getSqlAggFunction() {
        return SqlStdOperatorTable.COUNT;
    }


}
