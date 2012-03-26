/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.BaseQuery;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.PlanNode;

import java.util.ArrayDeque;
import java.util.Deque;

public final class ColumnEquivalenceStack {
    private static final int EQUIVS_DEQUE_SIZE = 3;
    private Deque<EquivalenceFinder<ColumnExpression>> stack
            = new ArrayDeque<EquivalenceFinder<ColumnExpression>>(EQUIVS_DEQUE_SIZE);
    
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
