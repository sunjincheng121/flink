package org.apache.calcite.converters.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.In;

public class InConverter implements RexNodeConverter<In> {
    @Override
    public RexNode toRexNode(In expr, RelBuilder relBuilder) {
        return null;
    }
}
