package org.apache.calcite.converters.expression;

import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Aggregation;

public interface AggCallConverter<T extends Aggregation> {
    RelBuilder.AggCall toAggCall(T agg, String name, boolean isDistinct, RelBuilder relBuilder);
}
