package org.apache.calcite.visitor;

import org.apache.flink.table.expressions.Cast;

public interface RexNodeVisitor <REXNODE> {
    REXNODE visit(Cast cast);
}
