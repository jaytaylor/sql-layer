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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Use join conditions to identify which tables are part of the same group.
 */
// TODO: Temporary until heuristic optimizer is removed.
public class GroupJoinFinder_Old extends GroupJoinFinder
{
    private static final Logger logger = LoggerFactory.getLogger(GroupJoinFinder_Old.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected void isolateGroups(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            TableGroup group = isolateGroups(island.root);
            if (group != null) {
                Joinable nroot = getTableJoins(island.root, group);
                island.output.replaceInput(island.root, nroot);
                island.root = nroot;
            }
        }
    }

    protected TableGroup isolateGroups(Joinable joinable) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            assert (table.getGroup() != null);
            return table.getGroup();
        }
        if (!joinable.isJoin())
            return null;
        // Both sides must be matching groups.
        JoinNode join = (JoinNode)joinable;
        Joinable left = join.getLeft();
        Joinable right = join.getRight();
        TableGroup leftGroup = isolateGroups(left);
        TableGroup rightGroup = isolateGroups(right);
        if ((leftGroup == rightGroup) && (leftGroup != null))
            return leftGroup;
        if (leftGroup != null)
            join.setLeft(getTableJoins(left, leftGroup));
        if (rightGroup != null)
            join.setRight(getTableJoins(right, rightGroup));
        // Make arbitrary joins LEFT not RIGHT.
        if (join.getJoinType() == JoinType.RIGHT)
            join.reverse();
        return null;
    }

    // Make a TableJoins from tables in a single TableGroup.
    protected Joinable getTableJoins(Joinable joins, TableGroup group) {
        TableJoins tableJoins = new TableJoins(joins, group);
        getTableJoinsTables(joins, tableJoins);
        return tableJoins;
    }

    protected void getTableJoinsTables(Joinable joinable, TableJoins tableJoins) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            getTableJoinsTables(join.getLeft(), tableJoins);
            getTableJoinsTables(join.getRight(), tableJoins);
        }
        else {
            assert joinable.isTable();
            tableJoins.addTable((TableSource)joinable);
        }
    }

    @Override
    protected void moveJoinConditions(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            moveJoinConditions(island.root, null, null, 
                               island.whereConditions, island.whereJoins);
        }        
    }

    protected void moveJoinConditions(Joinable joinable, JoinNode output, TableJoins tableJoins,
                                      ConditionList whereConditions, List<TableGroupJoin> whereJoins) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            TableGroupJoin tableJoin = table.getParentJoin();
            if (tableJoin != null) {
                if ((tableJoins == null) ||
                    !tableJoins.getTables().contains(tableJoin.getParent())) {
                    tableJoin.reject(); // Did not make it into the group.
                    if ((output != null) &&
                        (output.getGroupJoin() == tableJoin))
                        output.setGroupJoin(null);
                }
                else if (whereJoins.contains(tableJoin)) {
                    assert (output != null);
                    output.setGroupJoin(tableJoin);
                    List<ComparisonCondition> joinConditions = tableJoin.getConditions();
                    // Move down from WHERE conditions to join conditions.
                    if (output.getJoinConditions() == null)
                        output.setJoinConditions(new ConditionList());
                    output.getJoinConditions().addAll(joinConditions);
                    whereConditions.removeAll(joinConditions);
                }
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            moveJoinConditions(join.getLeft(), join, tableJoins, whereConditions, whereJoins);
            moveJoinConditions(join.getRight(), join, tableJoins, whereConditions, whereJoins);
        }
        else if (joinable instanceof TableJoins) {
            tableJoins = (TableJoins)joinable;
            moveJoinConditions(tableJoins.getJoins(), output, tableJoins, whereConditions, whereJoins);
        }
    }

    @Override
    protected boolean tableAllowedInGroup(TableGroup group, TableSource childTable) {
        // TODO: Avoid duplicate group joins. Really, they should be
        // recognized but only one allowed to Flatten and the other
        // forced to use a nested loop, but still with BranchLookup.
        for (TableSource otherChild : group.getTables()) {
            if ((otherChild.getTable() == childTable.getTable()) &&
                (otherChild != childTable))
                return false;
        }
        return true;
    }

}
