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

/** A join to a subquery result. */
public class SubquerySource extends BaseJoinable implements ColumnSource
{
    private PlanNode subquery;
    private String name;

    public SubquerySource(PlanNode subquery, String name) {
        this.subquery = subquery;
        this.name = name;
    }

    public PlanNode getSubquery() {
        return subquery;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            subquery.accept(v);
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        return super.summaryString() + "(" + name + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subquery = (PlanNode)subquery.duplicate(map);
    }

}
