package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ColumnSource;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.ExpressionVisitor;
import com.foundationdb.sql.optimizer.plan.PlanNode;
import com.foundationdb.sql.optimizer.plan.PlanVisitor;

import java.util.ArrayList;
import java.util.List;

/**
* Finds all the ColumnSources referenced by the given condition expression node
*/
public class ConditionColumnSourcesFinder implements PlanVisitor, ExpressionVisitor {
    List<ColumnSource> referencedSources;

    public ConditionColumnSourcesFinder() {
    }

    public List<ColumnSource> find(ExpressionNode expression) {
        referencedSources = new ArrayList<>();
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
            if (table instanceof ColumnSource) {
                referencedSources.add(table);
            }
        }
        return true;
    }
}
