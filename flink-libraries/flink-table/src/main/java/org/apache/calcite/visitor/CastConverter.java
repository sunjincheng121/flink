package org.apache.calcite.visitor;

import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.expressions.Cast;

public class CastConverter {
    public static RexNode toRexNode(Cast expr, RexNodeVisitorImpl visitor) {
        RexNode childRexNode =  expr.child().accept(visitor);
        FlinkTypeFactory typeFactory = (FlinkTypeFactory)visitor.relBuilder.getTypeFactory();
        return visitor.relBuilder.getRexBuilder().makeAbstractCast(
                typeFactory.createTypeFromTypeInfo(
                        expr.resultType(),
                        childRexNode.getType().isNullable()), childRexNode);
    }
}
