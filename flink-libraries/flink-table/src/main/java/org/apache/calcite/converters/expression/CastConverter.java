package org.apache.calcite.converters.expression;

import org.apache.calcite.converters.FlinkRexNodeConverterSet;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Cast;

public class CastConverter implements RexNodeConverter<Cast> {
    public static CastConverter INSTANCE = new CastConverter();

    @Override
    public RexNode toRexNode(Cast expr, RelBuilder relBuilder) {
        RexNode childRexNode = FlinkRexNodeConverterSet.toRexNode(expr.child(), relBuilder);

        FlinkTypeFactory typeFactory = (FlinkTypeFactory)relBuilder.getTypeFactory();

        return relBuilder.getRexBuilder().makeAbstractCast(
            typeFactory.createTypeFromTypeInfo(
                expr.resultType(),
                childRexNode.getType().isNullable()),
            childRexNode);
    }
}
