package org.apache.calcite.converters.expression.aggregations;

import org.apache.calcite.converters.FlinkAggCallConverterSet;
import org.apache.calcite.converters.expression.AggCallConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.DistinctAgg;

public class DistinctAggConverter implements AggCallConverter<DistinctAgg> {

    public static DistinctAggConverter INSTANCE = new DistinctAggConverter();

    @Override
    public RelBuilder.AggCall toAggCall(
        DistinctAgg expr, String name, boolean isDistinct, RelBuilder relBuilder) {
        Aggregation child = (Aggregation) expr.child();
        return FlinkAggCallConverterSet.toAggCall(child, name, true, relBuilder);
    }
}
