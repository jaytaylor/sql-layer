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

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.ais.model.Index;

import java.util.*;

public class IndexUsage extends BaseDuplicatable
{
    private Index index;
    private List<ConditionExpression> equalityConditions;
    private ConditionExpression lowCondition, highCondition;
    private List<OrderByExpression> ordering;

    // TODO: how successful: ordering, grouping, partial grouping, none
    // hkey depth returned?

    public IndexUsage(Index index) {
        this.index = index;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        equalityConditions = duplicateList(equalityConditions, map);
        if (lowCondition != null)
            lowCondition = (ConditionExpression)lowCondition.duplicate(map);
        if (highCondition != null)
            highCondition = (ConditionExpression)highCondition.duplicate(map);
        ordering = duplicateList(ordering, map);
    }
    
}
