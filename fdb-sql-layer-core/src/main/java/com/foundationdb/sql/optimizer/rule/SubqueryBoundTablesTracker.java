/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.*;

import java.util.*;

/** Annotate subqueries with their outer table references. */
public abstract class SubqueryBoundTablesTracker implements PlanVisitor, ExpressionVisitor {
    protected PlanContext planContext;
    protected BaseQuery rootQuery;
    protected Deque<SubqueryState> subqueries = new ArrayDeque<>();
    boolean trackingTables = true;


    protected SubqueryBoundTablesTracker(PlanContext planContext) {
        this.planContext = planContext;
    }

    protected void run() {
        rootQuery = (BaseQuery)planContext.getPlan();
        rootQuery.accept(this);
    }

    @Override
    public boolean visitEnter(PlanNode n) {
        if(n instanceof HashTableLookup)
            trackingTables = false;
        if (n instanceof Subquery) {
            subqueries.push(new SubqueryState((Subquery)n));
            return true;
        }
        return visit(n);
    }
    
    @Override
    public boolean visitLeave(PlanNode n) {
        if (n instanceof Subquery) {
            SubqueryState s = subqueries.pop();
            Set<ColumnSource> outerTables = s.getTablesReferencedButNotDefined();
            s.subquery.setOuterTables(outerTables);
            if (!subqueries.isEmpty())
                subqueries.peek().tablesReferenced.addAll(outerTables);
        }
        if(n instanceof HashTableLookup)
            trackingTables = true;
        return true;
    }

    @Override
    public boolean visit(PlanNode n) {
        if (trackingTables && !subqueries.isEmpty() &&
            (n instanceof ColumnSource)) {
            boolean added = subqueries.peek().tablesDefined.add((ColumnSource)n);
            assert added : "Table defined more than once";
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
        if (!subqueries.isEmpty() &&
            (n instanceof ColumnExpression)) {
            subqueries.peek().tablesReferenced.add(((ColumnExpression)n).getTable());
        }
        return true;
    }

    protected BaseQuery currentQuery() {
        if (subqueries.isEmpty()) {
            return rootQuery;
        }
        else {
            return subqueries.peek().subquery;
        }
    }

    static class SubqueryState {
        Subquery subquery;
        Set<ColumnSource> tablesReferenced = new HashSet<>();
        Set<ColumnSource> tablesDefined = new HashSet<>();

        public SubqueryState(Subquery subquery) {
            this.subquery = subquery;
        }

        public Set<ColumnSource> getTablesReferencedButNotDefined() {
            tablesReferenced.removeAll(tablesDefined);
            return tablesReferenced;
        }
    }
}
