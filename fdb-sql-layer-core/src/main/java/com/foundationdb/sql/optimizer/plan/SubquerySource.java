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

/** A join to a subquery result. */
public class SubquerySource extends BaseJoinable implements ColumnSource, PlanWithInput
{
    private Subquery subquery;
    private String name;

    public SubquerySource(Subquery subquery, String name) {
        this.subquery = subquery;
        this.name = name;
        subquery.setOutput(this);
    }

    public Subquery getSubquery() {
        return subquery;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (subquery == oldInput) {
            subquery = (Subquery)newInput;
            subquery.setOutput(this);
        }
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
    public String summaryString(PlanToString.Configuration configuration) {
        return super.summaryString(configuration) + "(" + name + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subquery = (Subquery)subquery.duplicate(map);
    }

}
