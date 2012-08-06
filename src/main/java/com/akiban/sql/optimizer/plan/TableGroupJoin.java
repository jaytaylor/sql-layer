/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Join;

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
