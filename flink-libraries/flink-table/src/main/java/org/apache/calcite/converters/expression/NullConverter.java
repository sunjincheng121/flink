package org.apache.calcite.converters.expression;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Null;

public class NullConverter implements RexNodeConverter<Null> {

    public static NullConverter INSTANCE = new NullConverter();

    @Override
    public RexNode toRexNode(Null expr, RelBuilder relBuilder) {
        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        FlinkTypeFactory typeFactory = (FlinkTypeFactory)relBuilder.getTypeFactory();
        return rexBuilder
            .makeCast(
                typeFactory.createTypeFromTypeInfo(expr.resultType(), true),
                rexBuilder.constantNull());
    }
}
