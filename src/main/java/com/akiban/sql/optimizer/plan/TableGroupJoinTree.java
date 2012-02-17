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

import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

/** Joins within a single {@link TableGroup} represented as a tree
 * whose structure mirrors that of the group.
 * This is an intermediate form between the original tree of joins based on the
 * original SQL syntax and the <code>Scan</code> and <code>Lookup</code> form
 * once an access path has been chosen.
 */
public class TableGroupJoinTree extends BaseJoinable
{
    public static class TableGroupJoinNode {
        TableSource table;
        TableGroupJoinNode parent, nextSibling, firstChild;
        JoinType parentJoinType;

        public TableGroupJoinNode(TableSource table) {
            this.table = table;
        }

        public TableSource getTable() {
            return table;
        }

        public TableGroupJoinNode getParent() {
            return parent;
        }
        public void setParent(TableGroupJoinNode parent) {
            this.parent = parent;
        }
        public TableGroupJoinNode getNextSibling() {
            return nextSibling;
        }
        public void setNextSibling(TableGroupJoinNode nextSibling) {
            this.nextSibling = nextSibling;
        }
        public TableGroupJoinNode getFirstChild() {
            return firstChild;
        }
        public void setFirstChild(TableGroupJoinNode firstChild) {
            this.firstChild = firstChild;
        }
        public JoinType getParentJoinType() {
            return parentJoinType;
        }
    }

    private TableGroup group;
    private TableGroupJoinNode root;
    
    public TableGroupJoinTree(TableGroup group, TableGroupJoinNode root) {
        this.group = group;
        this.root = root;
    }

    public TableGroup getGroup() {
        return group;
    }
    public TableGroupJoinNode getRoot() {
        return root;
    }
    
    public boolean accept(PlanVisitor v) {
        TableGroupJoinNode node = root;
        while (true) {
            if (v.visitEnter(node.getTable())) {
                TableGroupJoinNode next = node.getFirstChild();
                if (next != null) {
                    node = next;
                    continue;
                }
            }
            while (true) {
                if (v.visitLeave(node.getTable())) {
                    TableGroupJoinNode next = node.getNextSibling();
                    if (next != null) {
                        node = next;
                        break;
                    }
                }
                node = node.getParent();
                if (node == null)
                    return true;
            }
        }
    }

    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(")");
        return str.toString();
    }

}
