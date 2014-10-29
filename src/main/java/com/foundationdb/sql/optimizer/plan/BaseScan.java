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

package com.foundationdb.sql.optimizer.plan;

import java.util.Collection;
import java.util.Set;

public abstract class BaseScan extends BasePlanNode
{
    // Tables that would still need to be fetched if this scan were used.
    private Set<TableSource> requiredTables;
    
    // Estimated cost of using this scan.
    private CostEstimate costEstimate;

    public Set<TableSource> getRequiredTables() {
        return requiredTables;
    }
    public void setRequiredTables(Set<TableSource> requiredTables) {
        this.requiredTables = requiredTables;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }
    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
    }

    public abstract Collection<? extends ConditionExpression> getConditions();
    public abstract void visitComparands(ExpressionRewriteVisitor v);
    public abstract void visitComparands(ExpressionVisitor v);

}
