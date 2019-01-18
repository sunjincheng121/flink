package org.apache.calcite.converters;

import org.apache.calcite.converters.expression.AggCallConverter;
import org.apache.calcite.converters.expression.aggregations.DistinctAggConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.DistinctAgg;

import java.util.HashMap;
import java.util.Map;

public class FlinkAggCallConverterSet {
    private static Map<Class<? extends Aggregation>, AggCallConverter> expressionClass2ConverterMap
        = new HashMap<>();

    public static void registerConverter(
        Class<? extends Aggregation> expressionClass, AggCallConverter converter) {
        expressionClass2ConverterMap.put(expressionClass, converter);
    }

    static {
        registerConverter(DistinctAgg.class, DistinctAggConverter.INSTANCE);
    }

    public static RelBuilder.AggCall toAggCall(
        Aggregation expr, String name, boolean isDistinct, RelBuilder relBuilder) {
        AggCallConverter converter = expressionClass2ConverterMap.get(expr.getClass());
        if (converter != null)  {
            return converter.toAggCall(expr, name, isDistinct, relBuilder);
        } else {
            Class clazz = expr.getClass();
            while (clazz.getSuperclass() != null) {
                clazz = clazz.getSuperclass();
                converter = expressionClass2ConverterMap.get(clazz);
                if (converter != null) {
                    return converter.toAggCall(expr, name, isDistinct, relBuilder);
                }
            }
            throw new RuntimeException(expr.getClass().getCanonicalName() + " is not supported!");
        }
    }
}
