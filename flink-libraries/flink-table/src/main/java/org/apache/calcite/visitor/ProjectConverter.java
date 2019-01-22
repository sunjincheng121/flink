package org.apache.calcite.visitor;

import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.expressions.Alias;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.NamedExpression;
import org.apache.flink.table.plan.logical.Project;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;


public class ProjectConverter {
    public static void construct(Project project, RelNodeVisitorImpl relNodeVisitor) {
        List<NamedExpression> namedExpressions =
                JavaConversions.seqAsJavaList(project.projectList());

        List<Expression> expr = new ArrayList<>();
        if (project.explicitAlias()) {
            expr.addAll(namedExpressions);
        } else {
            namedExpressions.forEach(
                    (NamedExpression e) -> {
                        // remove AS expressions, according to Calcite they should not be in a final RexNode
                        if (e instanceof Alias) {
                            expr.add(((Alias)e).child());
                        } else {
                            expr.add(e);
                        }
                    }
            );
        }

        List<String> exprNames = new ArrayList<>();
        for (NamedExpression e: namedExpressions) {
            exprNames.add(e.name());
        }

        List<RexNode> rexNodes = new ArrayList<>();

        for (Expression e: expr) {
            rexNodes.add(e.accept(new RexNodeVisitorImpl(relNodeVisitor.relBuilder)));
        }

        relNodeVisitor.relBuilder.project(
                rexNodes,
                exprNames,
                true);
    }
}
