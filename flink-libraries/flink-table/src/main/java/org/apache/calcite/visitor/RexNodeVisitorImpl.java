package org.apache.calcite.visitor;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.expressions.Cast;

public class RexNodeVisitorImpl implements RexNodeVisitor<RexNode> {
    public RelBuilder relBuilder;

    public RexNodeVisitorImpl(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
    }

    public RexNode visit(Cast cast) {
        return CastConverter.toRexNode(cast, this);
    }

}
