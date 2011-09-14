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

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** A goal for indexing: conditions on joined tables and ordering / grouping. */
public class PickIndexes extends BaseRule
{
    @Override
    public PlanNode apply(PlanNode plan) {
        List<Joinable> islands = new FindGroupJoins.JoinIslandFinder().find(plan);
        // TODO: For now, very conservative about everything being simple.
        if (islands.isEmpty()) return plan;
        if (islands.size() > 1)
            throw new UnsupportedSQLException("Joins are too complex: " + islands, null);
        Joinable joins = islands.get(0);
        pickIndexes(joins);
        return plan;
    }

    protected void pickIndexes(Joinable joins) {
        
    }
}