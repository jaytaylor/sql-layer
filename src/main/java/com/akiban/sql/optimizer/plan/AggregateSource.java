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

package com.akiban.sql.optimizer.plan;

import java.util.List;

public class AggregateSource extends JoinSource
{
    private PlanNode input;
    private List<BaseExpression> groupBy;
    private List<AggregateExpression> aggregates;

    public AggregateSource(PlanNode input,
                           List<BaseExpression> groupBy) {
        this.input = input;
        this.groupBy = groupBy;
        this.aggregates = new ArrayList<AggregateExpression>();
    }

    public List<BaseExpression> getGroupBy() {
        return groupBy;
    }
    public List<AggregateExpression> getAggregates() {
        return aggregates;
    }

    @Override
    public String toString() {
        return "GROUP BY " + groupBy + aggregates + "\n" + input;
    }

}
