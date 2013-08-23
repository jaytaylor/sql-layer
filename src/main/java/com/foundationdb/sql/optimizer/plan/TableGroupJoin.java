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

import com.foundationdb.ais.model.Join;

import java.util.List;

/** A join within a group corresponding to the GROUPING FK constraint. 
 */
public class TableGroupJoin extends BasePlanElement
{
    private TableGroup group;
    private TableSource parent, child;
    private List<ComparisonCondition> conditions;
    private Join join;

    public TableGroupJoin(TableGroup group,
                          TableSource parent, TableSource child,
                          List<ComparisonCondition> conditions, Join join) {
        this.group = group;
        this.parent = parent;
        parent.setGroup(group);
        this.child = child;
        this.conditions = conditions;
        for (ComparisonCondition condition : conditions)
            condition.setImplementation(ConditionExpression.Implementation.GROUP_JOIN);
        child.setParentJoin(this);
        this.join = join;
        group.addJoin(this);
    }

    public TableGroup getGroup() {
        return group;
    }
    protected void setGroup(TableGroup group) {
        this.group = group;
    }

    public TableSource getParent() {
        return parent;
    }
    public TableSource getChild() {
        return child;
    }
    public List<ComparisonCondition> getConditions() {
        return conditions;
    }
    public Join getJoin() {
        return join;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            "(" + join + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate(map);
        parent = (TableSource)parent.duplicate(map);
        child = (TableSource)child.duplicate(map);
        conditions = duplicateList(conditions, map);
    }
    
    /** When a group join is across a continguous set of join, it
     * isn't part of the group any more. It is still specially marked
     * for consideration of group operators as access paths in a
     * regular join.
     */
    public void reject() {
        for (ComparisonCondition condition : conditions)
            condition.setImplementation(ConditionExpression.Implementation.POTENTIAL_GROUP_JOIN);
        child.setParentJoin(null);
        group.rejectJoin(this);
    }

}
