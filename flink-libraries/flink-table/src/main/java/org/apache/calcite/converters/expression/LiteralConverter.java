package org.apache.calcite.converters.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Literal;

public class LiteralConverter implements RexNodeConverter<Literal> {
    @Override
    public RexNode toRexNode(Literal expr, RelBuilder relBuilder) {
        return null;
    }
}
