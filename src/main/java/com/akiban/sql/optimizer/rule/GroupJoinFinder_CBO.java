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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;
import com.akiban.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.akiban.server.expression.std.Comparison;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Use join conditions to identify which tables are part of the same group.
 */
// TODO: This temporarily has just the methods that are different in the CBO arrangement.
// These should be merged into the single class when this becomes the default.
public class GroupJoinFinder_CBO extends GroupJoinFinder
{
    private static final Logger logger = LoggerFactory.getLogger(GroupJoinFinder_CBO.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected boolean tableAllowedInGroup(TableGroup group, TableSource childTable) {
        return true;
    }

    @Override
    protected void isolateGroups(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            TableGroupJoinNode tree = isolateGroupJoins(island.root);
            if (tree != null) {
                Joinable nroot = groupJoinTree(tree, island.root);
                island.output.replaceInput(island.root, nroot);
                island.root = nroot;
            }
        }
    }

    protected TableGroupJoinNode isolateGroupJoins(Joinable joinable) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            assert (table.getGroup() != null);
            return new TableGroupJoinNode(table);
        }
        if (!joinable.isJoin())
            return null;
        JoinNode join = (JoinNode)joinable;
        Joinable left = join.getLeft();
        Joinable right = join.getRight();
        TableGroupJoinNode leftTree = isolateGroupJoins(left);
        TableGroupJoinNode rightTree = isolateGroupJoins(right);
        if ((leftTree != null) && (rightTree != null) &&
            // All tables below the two sides must be from the same group.
            (leftTree.getTable().getGroup() == rightTree.getTable().getGroup())) {
            // An outer join condition must be one that can be
            // done before flattening because after that it's too
            // late to get back the outer side if the test never
            // succeeds.
            boolean joinOK;
            switch (join.getJoinType()) {
            case INNER:
                joinOK = true;
                break;
            case LEFT:
                joinOK = checkJoinConditions(join.getJoinConditions(), leftTree, rightTree);
                break;
            case RIGHT:
                // Cannot allow any non-group conditions, since even
                // one only on parent would kill the child because
                // that's how Select_HKeyOrdered works.
                joinOK = checkJoinConditions(join.getJoinConditions(), null, leftTree);
                break;
            default:
                joinOK = false;
            }
            if (joinOK) {
                // Still need to be able to splice them together.
                TableGroupJoinNode tree;
                int leftDepth = leftTree.getTable().getTable().getDepth();
                int rightDepth = rightTree.getTable().getTable().getDepth();
                if (leftDepth < rightDepth)
                    tree = spliceGroupJoins(leftTree, rightTree, join.getJoinType());
                else if (rightDepth < leftDepth)
                    tree = spliceGroupJoins(rightTree, leftTree, join.getJoinType());
                else
                    tree = null;
                if (tree != null) {
                    return tree;
                }
            }
        }
        // Did not manage to coalesce. Put in any intermediate trees.
        if (leftTree != null)
            join.setLeft(groupJoinTree(leftTree, left));
        if (rightTree != null)
            join.setRight(groupJoinTree(rightTree, right));
        // Make arbitrary joins LEFT not RIGHT.
        if (join.getJoinType() == JoinType.RIGHT)
            join.reverse();
        return null;
    }

    protected TableGroupJoinTree groupJoinTree(TableGroupJoinNode root, Joinable joins) {
        TableGroupJoinTree tree = new TableGroupJoinTree(root);
        Set<TableSource> required = new HashSet<TableSource>();
        getRequiredTables(joins, required);
        tree.setRequired(required);
        return tree;
    }

    // Get all the tables reachable via inner joins from here.
    protected void getRequiredTables(Joinable joinable, Set<TableSource> required) {
        if (joinable instanceof TableSource) {
            required.add((TableSource)joinable);
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            if (join.getJoinType() != JoinType.RIGHT)
                getRequiredTables(join.getLeft(), required);
            if (join.getJoinType() != JoinType.LEFT)
                getRequiredTables(join.getRight(), required);
        }
    }

    // Combine trees at the proper branch point.
    protected TableGroupJoinNode spliceGroupJoins(TableGroupJoinNode parent, 
                                                  TableGroupJoinNode child,
                                                  JoinType joinType) {
        TableGroupJoinNode branch = parent.findTable(child.getTable().getParentTable());
        if (branch == null)
            return null;
        child.setParent(branch);
        child.setParentJoinType(joinType);
        TableGroupJoinNode prev = null;
        while (true) {
            TableGroupJoinNode next = (prev == null) ? branch.getFirstChild() : prev.getNextSibling();
            if ((next == null) || 
                (next.getTable().getTable().getOrdinal() > child.getTable().getTable().getOrdinal())) {
                child.setNextSibling(next);
                if (prev == null)
                    branch.setFirstChild(child);
                else
                    prev.setNextSibling(child);
                break;
            }
            prev = next;
        }
        return parent;
    }

    protected boolean checkJoinConditions(ConditionList joinConditions,
                                          TableGroupJoinNode outer,
                                          TableGroupJoinNode inner) {
        if (hasIllegalReferences(joinConditions, outer))
            return false;
        inner.setJoinConditions(joinConditions);
        return true;
    }

    // See whether any expression in the join condition other than the
    // grouping join references a table under the given tree.
    protected boolean hasIllegalReferences(ConditionList joinConditions,
                                           TableGroupJoinNode fromTree) {
        JoinedReferenceFinder finder = null;
        if (joinConditions != null) {
            for (ConditionExpression condition : joinConditions) {
                if (condition.getImplementation() == ConditionExpression.Implementation.GROUP_JOIN)
                    continue;   // Group condition okay.
                if (fromTree == null)
                    return true; // All non-group disallowed.
                if (finder == null)
                    finder = new JoinedReferenceFinder(fromTree);
                if (finder.find(condition))
                    return true; // Has references to other side.
            }
        }
        return false;
    }

    static class JoinedReferenceFinder implements PlanVisitor, ExpressionVisitor {
        private TableGroupJoinNode fromTree;
        private boolean found;

        public JoinedReferenceFinder(TableGroupJoinNode fromTree) {
            this.fromTree = fromTree;
        }

        public boolean find(ExpressionNode expression) {
            found = false;
            expression.accept(this);
            return found;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                ColumnSource table = ((ColumnExpression)n).getTable();
                if (table instanceof TableSource) {
                    if (fromTree.findTable((TableSource)table) != null) {
                        found = true;
                    }
                }
            }
            return true;
        }
    }

    @Override
    protected Joinable getTableJoins(Joinable joins, TableGroup group) {
        throw new UnsupportedOperationException("Should have made TableGroupJoinTree");
    }

    @Override
    protected void moveJoinConditions(List<JoinIsland> islands) {
        for (JoinIsland island : islands) {
            moveJoinConditions(island.root, island.whereConditions, island.whereJoins);
        }        
    }

    protected void moveJoinConditions(Joinable joinable,
                                      ConditionList whereConditions, List<TableGroupJoin> whereJoins) {
        if (joinable instanceof TableGroupJoinTree) {
            for (TableGroupJoinNode table : (TableGroupJoinTree)joinable) {
                TableGroupJoin tableJoin = table.getTable().getParentJoin();
                if (tableJoin != null) {
                    if (table.getParent() == null) {
                        tableJoin.reject(); // Did not make it into the group.
                    }
                    else if (whereJoins.contains(tableJoin)) {
                        List<ComparisonCondition> joinConditions = tableJoin.getConditions();
                        // Move down from WHERE conditions to join conditions.
                        if (table.getJoinConditions() == null)
                            table.setJoinConditions(new ConditionList());
                        table.getJoinConditions().addAll(joinConditions);
                        whereConditions.removeAll(joinConditions);
                    }
                }
            }
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            join.setGroupJoin(null);
            moveJoinConditions(join.getLeft(), whereConditions, whereJoins);
            moveJoinConditions(join.getRight(), whereConditions, whereJoins);
        }
    }
    
    @Override
    protected void moveJoinConditions(Joinable joinable, JoinNode output, TableJoins tableJoins,
                                      ConditionList whereConditions, List<TableGroupJoin> whereJoins) {
        throw new UnsupportedOperationException("Should have avoided TableJoins");
    }

}
