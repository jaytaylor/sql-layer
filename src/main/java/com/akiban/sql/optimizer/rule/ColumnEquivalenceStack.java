
package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.BaseQuery;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.PlanNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class ColumnEquivalenceStack {
    private static final int EQUIVS_DEQUE_SIZE = 3;
    private Deque<EquivalenceFinder<ColumnExpression>> stack
            = new ArrayDeque<>(EQUIVS_DEQUE_SIZE);
    
    public boolean enterNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            BaseQuery bq = (BaseQuery) n;
            stack.push(bq.getColumnEquivalencies());
            return true;
        }
        return false;
    }
    
    public EquivalenceFinder<ColumnExpression> leaveNode(PlanNode n) {
        if (n instanceof BaseQuery) {
            return stack.pop();
        }
        return null;
    }
    
    public EquivalenceFinder<ColumnExpression> get() {
        return stack.element();
    }

    public boolean isEmpty() {
        return stack.isEmpty();
    }
}
