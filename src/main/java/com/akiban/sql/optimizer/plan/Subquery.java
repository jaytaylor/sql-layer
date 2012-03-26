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

import com.akiban.sql.optimizer.rule.EquivalenceFinder;

import java.util.Set;

/** A marker node around some subquery.
 */
public class Subquery extends BaseQuery
{
    private Set<ColumnSource> outerTables;

    public Subquery(PlanNode inside, EquivalenceFinder<ColumnExpression> columnEquivalencies) {
        super(inside, columnEquivalencies);
    }

    @Override
    public Set<ColumnSource> getOuterTables() {
        if (outerTables != null)
            return outerTables;
        else
            return super.getOuterTables();
    }

    public void setOuterTables(Set<ColumnSource> outerTables) {
        this.outerTables = outerTables;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (outerTables != null)
            outerTables = duplicateSet(outerTables, map);
    }

}
