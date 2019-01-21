package org.apache.calcite.converters;

import org.apache.calcite.converters.expression.AggregationConverter;
import org.apache.calcite.converters.expression.aggregations.DistinctAggConverter;
import org.apache.calcite.converters.expression.aggregations.SumConverter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Aggregation;
import org.apache.flink.table.expressions.DistinctAgg;
import org.apache.flink.table.expressions.Sum;

import java.util.HashMap;
import java.util.Map;

public class FlinkAggCallConverterSet {
    private static Map<Class<? extends Aggregation>, AggregationConverter> expressionClass2ConverterMap
        = new HashMap<>();

    public static <T extends Aggregation> void registerConverter(
        Class<T> expressionClass, AggregationConverter<T> converter) {
        expressionClass2ConverterMap.put(expressionClass, converter);
    }

    static {
        registerConverter(DistinctAgg.class, DistinctAggConverter.INSTANCE);
        registerConverter(Sum.class, SumConverter.INSTANCE);
    }

    public static RelBuilder.AggCall toAggCall(
        Aggregation agg, String name, boolean isDistinct, RelBuilder relBuilder) {
        AggregationConverter converter = expressionClass2ConverterMap.get(agg.getClass());
        if (converter != null)  {
            return converter.toAggCall(agg, name, isDistinct, relBuilder);
        } else {
            Class clazz = agg.getClass();
            while (clazz.getSuperclass() != null) {
                clazz = clazz.getSuperclass();
                converter = expressionClass2ConverterMap.get(clazz);
                if (converter != null) {
                    return converter.toAggCall(agg, name, isDistinct, relBuilder);
                }
            }
            throw new RuntimeException(agg.getClass().getCanonicalName() + " is not supported. "
                + "You need to register a " +
                AggregationConverter.class.getSimpleName() + "<" + agg.getClass().getSimpleName() + ">"
                + "in " + FlinkAggCallConverterSet.class.getSimpleName() + " to fix this problem.");
        }
    }

    public static SqlAggFunction getSqlAggFunction(
        Aggregation agg, RelBuilder relBuilder) {
        AggregationConverter converter = expressionClass2ConverterMap.get(agg.getClass());
        if (converter != null)  {
            return converter.getSqlAggFunction(agg, relBuilder);
        } else {
            Class clazz = agg.getClass();
            while (clazz.getSuperclass() != null) {
                clazz = clazz.getSuperclass();
                converter = expressionClass2ConverterMap.get(clazz);
                if (converter != null) {
                    return converter.getSqlAggFunction(agg, relBuilder);
                }
            }
            throw new RuntimeException(agg.getClass().getCanonicalName() + " is not supported. "
                + "You need to register a " +
                AggregationConverter.class.getSimpleName() + "<" + agg.getClass().getSimpleName() + ">"
                + "in " + FlinkAggCallConverterSet.class.getSimpleName() + " to fix this problem.");
        }
    }
}
