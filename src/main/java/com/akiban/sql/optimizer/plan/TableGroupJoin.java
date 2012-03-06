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

import com.akiban.ais.model.Join;
import com.akiban.sql.optimizer.plan.ConditionExpression.Implementation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A join within a group corresponding to the GROUPING FK constraint. 
 */
public class TableGroupJoin extends BasePlanElement
{
    private TableGroup group;
    private TableSource parent, child;
    private NormalizedConditions conditions;
    private Join join;
    private ConditionList originalConditions;

    public TableGroupJoin(TableGroup group,
                          TableSource parent, TableSource child,
                          ConditionList originalConditions,
                          NormalizedConditions conditions, Join join) {
        this.group = group;
        this.parent = parent;
        parent.setGroup(group);
        this.child = child;
        this.conditions = conditions;
        this.originalConditions = originalConditions;
        this.originalConditions.removeAll(conditions.original);
        for (ComparisonCondition cond : conditions.normalized) {
            cond.setImplementation(ConditionExpression.Implementation.GROUP_JOIN);
            this.originalConditions.add(cond);
        }
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
        return conditions.normalized;
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
        conditions = new NormalizedConditions(
                duplicateList(conditions.normalized, map),
                duplicateList(conditions.original, map)
        );
    }
    
    /** When a group join is across a continguous set of join, it
     * isn't part of the group any more. It is still specially marked
     * for consideration of group operators as access paths in a
     * regular join.
     */
    public void reject() {
        originalConditions.removeAll(conditions.normalized);
        for (ComparisonCondition cond : conditions.original) {
            cond.setImplementation(ConditionExpression.Implementation.POTENTIAL_GROUP_JOIN);
            originalConditions.add(cond);
        }
        child.setParentJoin(null);
        group.getJoins().remove(this);
    }
    
    public static class NormalizedConditions {
        private List<ComparisonCondition> normalized;
        private List<ComparisonCondition> original;

        private NormalizedConditions(List<ComparisonCondition> normalized, List<ComparisonCondition> original) {
            this.normalized = normalized;
            this.original = original;
        }

        public NormalizedConditions(int ncols) {
            List<ComparisonCondition> empty = Collections.nCopies(ncols, null);
            this.normalized = new ArrayList<ComparisonCondition>(empty);
            this.original = new ArrayList<ComparisonCondition>(empty);
        }

        public void set(int index, ComparisonCondition normalized, ComparisonCondition original) {
            this.normalized.set(index,  normalized);
            this.original.set(index, original);
        }
        
        public List<ComparisonCondition> getNormalized() {
            return normalized;
        }
        
        public boolean allColumnsSet() {
            for (ComparisonCondition elem : normalized) {
                if (elem == null)
                    return false;
            }
            return true;
        }
    }
}
