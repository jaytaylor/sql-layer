package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ColumnSource;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.ExpressionVisitor;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;

import java.util.HashSet;
import java.util.Set;

/**
* Finds all the ColumnSources referenced by the given condition expression node
*/
public class ConditionColumnSourcesFinder implements PlanVisitor, ExpressionVisitor {
    Set<ColumnSource> referencedSources;

    public ConditionColumnSourcesFinder() {
    }

    public Set<ColumnSource> find(ExpressionNode expression) {
        referencedSources = new HashSet<>();
        expression.accept(this);
        return referencedSources;
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
        if (n instanceof ColumnExpression) {
            ColumnSource table = ((ColumnExpression)n).getTable();
            referencedSources.add(table);
        }
        return true;
    }
}
