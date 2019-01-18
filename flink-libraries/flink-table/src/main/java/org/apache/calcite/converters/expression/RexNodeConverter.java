package org.apache.calcite.converters.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Expression;

public interface RexNodeConverter<T extends Expression> {
    RexNode toRexNode(T expr, RelBuilder relBuilder);
}
