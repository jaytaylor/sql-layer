
package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.sql.optimizer.plan.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** {@link Sort} takes a list of expressions, unlike {@link
 * AggregateSource}, which takes value rows from {@link Project}. If
 * the Sort would be pushed into a {@link MapJoin}, it needs to get
 * {@link Project} so that can go on the inside and it can remain
 * outside. */
public class SortSplitter extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(SortSplitter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        List<Sort> sorts = new MapSortFinder().find(plan.getPlan());
        for (Sort sort : sorts) {
            split(sort);
        }
    }

    protected void split(Sort sort) {
        PlanNode input = sort.getInput();
        if (input instanceof Project)
            return;             // Already all set.
        Project outputProject = null;
        List<ExpressionNode> projects = new ArrayList<>();
        Project inputProject = new Project(input, projects);
        boolean projectsAdded = false;
        if (sort.getOutput() instanceof Project) {
            outputProject = (Project)sort.getOutput();
            projects.addAll(outputProject.getFields());
        }
        for (OrderByExpression orderBy : sort.getOrderBy()) {
            ExpressionNode expr = orderBy.getExpression();
            int idx;
            if (outputProject == null)
                idx = -1;
            else
                idx = projects.indexOf(expr);
            if (idx < 0) {
                idx = projects.size();
                projects.add(expr);
                projectsAdded = true;
            }
            ExpressionNode cexpr = new ColumnExpression(inputProject, idx,
                                                        expr.getSQLtype(),
                                                        expr.getSQLsource());
            orderBy.setExpression(cexpr);
        }
        sort.replaceInput(input, inputProject);
        if (outputProject != null) {
            if (!projectsAdded) {
                // If we sorted by a subset of the output projects (in any
                // order), since we arranged for the input projects to
                // match, we don't need that second project any more.
                outputProject.getOutput().replaceInput(outputProject, sort);
            }
            else {
                // If we sorted by something we didn't output, we
                // still need the project, so it has to be reformed in
                // terms of the superset one.
                List<ExpressionNode> oprojects = outputProject.getFields();
                for (int i = 0; i < oprojects.size(); i++) {
                    ExpressionNode oexpr = oprojects.get(i);
                    ExpressionNode cexpr = new ColumnExpression(inputProject, i,
                                                                oexpr.getSQLtype(),
                                                                oexpr.getSQLsource());
                    oprojects.set(i, cexpr);
                }
            }
        }
    }

    static class MapSortFinder implements PlanVisitor, ExpressionVisitor {
        List<Sort> result = new ArrayList<>();

        public List<Sort> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof MapJoin) {
                PlanNode output = n.getOutput();
                if (output instanceof Select)
                    output = output.getOutput();
                if (output instanceof Sort)
                    result.add((Sort)output);
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

}
