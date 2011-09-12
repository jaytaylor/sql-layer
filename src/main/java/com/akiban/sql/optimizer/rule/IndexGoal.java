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

import com.akiban.sql.optimizer.plan.*;

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.*;

public class IndexGoal
{
    // The group we want to index.
    private TableGroup group;
    // All the tables that are joined together here, including outer
    // join ones that can't have indexes, but might have sorting or
    // covering.
    private List<TableSource> tables;
    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;

    // If both grouping and ordering are present, they must be
    // compatible: something satisfying the ordering would also handle
    // the grouping.
    private List<ExpressionNode> grouping;
    private List<OrderByExpression> ordering;

    public IndexGoal(TableGroup group) {
    }
}
