package org.apache.calcite.visitor;

import org.apache.flink.table.plan.logical.Project;

public interface RelNodeVisitor {
   void visit(Project project);
}
