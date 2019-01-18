package org.apache.calcite.converters;

import org.apache.calcite.converters.expression.CastConverter;
import org.apache.calcite.converters.expression.NullConverter;
import org.apache.calcite.converters.expression.RexNodeConverter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Cast;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.Null;

import java.util.HashMap;
import java.util.Map;

public class FlinkRexNodeConverterSet {
    private static Map<Class<? extends Expression>, RexNodeConverter> expressionClass2ConverterMap
        = new HashMap<>();

    public static void registerConverter(
        Class<? extends Expression> expressionClass, RexNodeConverter converter) {
        expressionClass2ConverterMap.put(expressionClass, converter);
    }

    static {
        registerConverter(Cast.class, CastConverter.INSTANCE);
        registerConverter(Null.class, NullConverter.INSTANCE);
    }

    public static RexNode toRexNode(Expression expr, RelBuilder relBuilder) {
        RexNodeConverter converter = expressionClass2ConverterMap.get(expr.getClass());
        if (converter != null)  {
            return converter.toRexNode(expr, relBuilder);
        } else {
            Class clazz = expr.getClass();
            while (clazz.getSuperclass() != null) {
                clazz = clazz.getSuperclass();
                converter = expressionClass2ConverterMap.get(clazz);
                if (converter != null) {
                    return converter.toRexNode(expr, relBuilder);
                }
            }
            throw new RuntimeException(expr.getClass().getCanonicalName() + " is not supported!");
        }
    }
}
