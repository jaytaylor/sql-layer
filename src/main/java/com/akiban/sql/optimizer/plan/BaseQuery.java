/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.Collections;
import java.util.Set;

/** A statement / subquery.
 */
public class BaseQuery extends BasePlanWithInput
{
    private EquivalenceFinder<ColumnExpression> columnEquivalencies;
    private CostEstimate costEstimate;

    protected BaseQuery(PlanNode query, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(query);
        this.columnEquivalencies = columnEquivalencies;
    }

    public PlanNode getQuery() {
        return getInput();
    }

    public Set<ColumnSource> getOuterTables() {
        return Collections.<ColumnSource>emptySet();
    }

    public EquivalenceFinder<ColumnExpression> getColumnEquivalencies() {
        return columnEquivalencies;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }
    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
    }

}
