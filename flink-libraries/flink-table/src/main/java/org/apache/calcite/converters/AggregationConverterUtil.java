package org.apache.calcite.converters;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Count;
import org.apache.flink.table.expressions.DistinctAgg;

public class AggregationConverterUtil {

    public static RelBuilder.AggCall toAggCall(
            Count agg,
            String name,
            boolean isDistinct,
            RelBuilder relBuilder) {
        return CountConverter.toAggCall(agg, name, isDistinct, relBuilder);
    }


    public static SqlAggFunction getSqlAggFunction() {
        return CountConverter.getSqlAggFunction();
    }

    public static RelBuilder.AggCall toAggCall(
            DistinctAgg agg,
            String name,
            RelBuilder relBuilder) {
        return DistinctAggConverter.toAggCall(agg, name, relBuilder);
    }


    public static SqlAggFunction getSqlAggFunction(DistinctAgg agg, RelBuilder relBuilder) {
        return DistinctAggConverter.getSqlAggFunction(agg, relBuilder);
    }

}
