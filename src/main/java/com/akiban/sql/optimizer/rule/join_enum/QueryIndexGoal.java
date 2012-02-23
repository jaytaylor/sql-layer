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

package com.akiban.sql.optimizer.rule.join_enum;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.*;

/** The overall goal of a query: WHERE conditions, ORDER BY, etc. */
public class QueryIndexGoal
{
    private ConditionList whereConditions;

    // If both grouping and ordering are present, they must be
    // compatible. Something satisfying the ordering would also handle
    // the grouping. All the order by columns must also be group by
    // columns, though not necessarily in the same order. There can't
    // be any additional order by columns, because even though it
    // would be properly grouped going into aggregation, it wouldn't
    // still be sorted by those coming out. It's hard to write such a
    // query in SQL, since the ORDER BY can't contain columns not in
    // the GROUP BY, and non-columns won't appear in the index.
    private AggregateSource grouping;
    private Sort ordering;
    private Project projectDistinct;

    private TableNode updateTarget;

    public QueryIndexGoal(BaseQuery query,
                          ConditionList whereConditions,
                          AggregateSource grouping,
                          Sort ordering,
                          Project projectDistinct) {
        this.whereConditions = whereConditions;
        this.grouping = grouping;
        this.ordering = ordering;
        this.projectDistinct = projectDistinct;

        if ((query instanceof UpdateStatement) ||
            (query instanceof DeleteStatement))
          updateTarget = ((BaseUpdateStatement)query).getTargetTable();
    }

}
