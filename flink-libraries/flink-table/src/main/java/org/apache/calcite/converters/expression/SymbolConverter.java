package org.apache.calcite.converters.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.SymbolExpression;

public class SymbolConverter implements RexNodeConverter<SymbolExpression> {
    @Override
    public RexNode toRexNode(SymbolExpression expr, RelBuilder relBuilder) {
        return null;
    }
}
