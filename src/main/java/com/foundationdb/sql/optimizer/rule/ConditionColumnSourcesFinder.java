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
