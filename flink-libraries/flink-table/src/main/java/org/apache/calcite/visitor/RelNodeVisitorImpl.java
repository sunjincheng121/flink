package org.apache.calcite.visitor;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.plan.logical.LogicalNode;
import org.apache.flink.table.plan.logical.Project;

public class RelNodeVisitorImpl implements RelNodeVisitor {
    public RelBuilder relBuilder;

    public RelNodeVisitorImpl(RelBuilder relBuilder) {
        this.relBuilder = relBuilder;
    }

    public void visit(Project project) {
        ProjectConverter.construct(project, this);
    }


    public RelNode toRelNode(LogicalNode logicalNode) {
        logicalNode.accept(this);
        return relBuilder.build();
    }

}
