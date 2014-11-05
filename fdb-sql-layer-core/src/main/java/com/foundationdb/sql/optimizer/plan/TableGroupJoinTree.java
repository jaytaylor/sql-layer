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

import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/** Joins within a single {@link TableGroup} represented as a tree
 * whose structure mirrors that of the group.
 * This is an intermediate form between the original tree of joins based on the
 * original SQL syntax and the <code>Scan</code> and <code>Lookup</code> form
 * once an access path has been chosen.
 */
public class TableGroupJoinTree extends BaseJoinable 
    implements Iterable<TableGroupJoinTree.TableGroupJoinNode>, PlanWithInput
{
    public static class TableGroupJoinNode implements Iterable<TableGroupJoinNode> {
        TableSource table;
        TableGroupJoinNode parent, nextSibling, firstChild;
        JoinType parentJoinType;
        ConditionList joinConditions;
        long state;
        
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
        public void setParentJoinType(JoinType parentJoinType) {
            this.parentJoinType = parentJoinType;
        }

        public ConditionList getJoinConditions() {
            return joinConditions;
        }
        public void setJoinConditions(ConditionList joinConditions) {
            this.joinConditions = joinConditions;
        }

        /** Integer state managed by some rule. */
        public long getState() {
            return state;
        }
        public void setState(long state) {
            this.state = state;
        }

        /** Find the given table in this (sub-)tree. */
        public TableGroupJoinNode findTable(TableSource table) {
            for (TableGroupJoinNode node : this) {
                if (node.getTable() == table) {
                    return node;
                }
            }
            return null;
        }

        @Override
        public Iterator<TableGroupJoinNode> iterator() {
            return new TableGroupJoinIterator(this);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
                "(" + table + ", " + state + ")";
        }
    }

    static class TableGroupJoinIterator implements Iterator<TableGroupJoinNode> {
        TableGroupJoinNode root, next;
        
        TableGroupJoinIterator(TableGroupJoinNode root) {
            this.root = this.next = root;
        }

        @Override
        public boolean hasNext() {
            return (next != null);
        }

        @Override
        public TableGroupJoinNode next() {
            if (next == null)
                throw new NoSuchElementException();
            TableGroupJoinNode node = next;
            advance();
            return node;
        }
        
        protected void advance() {
            TableGroupJoinNode node = next.getFirstChild();
            if (node != null) {
                next = node;
                return;
            }
            while (true) {
                if (next == root) {
                    next = null;
                    return;
                }
                node = next.getNextSibling();
                if (node != null) {
                    next = node;
                    return;
                }
                next = next.getParent();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public interface LeafFinderPredicate<V> {
        boolean includeAndContinue(TableGroupJoinNode node, TableGroupJoinTree tree);
        V mapToValue(TableGroupJoinNode node);
    }
    
    private TableGroup group;
    private TableGroupJoinNode root;
    private Set<TableSource> required;
    private BaseScan scan;
    
    public TableGroupJoinTree(TableGroupJoinNode root) {
        this.group = root.getTable().getGroup();
        this.root = root;
    }

    public TableGroup getGroup() {
        return group;
    }
    public TableGroupJoinNode getRoot() {
        return root;
    }

    public Set<TableSource> getRequired() {
        return required;
    }
    public void setRequired(Set<TableSource> required) {
        this.required = required;
    }
    
    public BaseScan getScan() {
        return scan;
    }
    public void setScan(BaseScan scan) {
        this.scan = scan;
    }
    
    public boolean containsTable(TableSource table) {
        return (root.findTable(table) != null);
    }

    public <V> Map<TableGroupJoinNode,V> findLeaves(LeafFinderPredicate<V> predicate) {
        Map<TableGroupJoinNode,V> results = new HashMap<>();
        if (predicate.includeAndContinue(root, this))
            buildLeaves(root, predicate, results);
        return results;
    }

    @Override
    public Iterator<TableGroupJoinNode> iterator() {
        return new TableGroupJoinIterator(root);
    }

    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (scan != null) {
                // Could just do scan.accept(v), but then it'd explain
                // on a separate line.
                if (v instanceof ExpressionRewriteVisitor) {
                    scan.visitComparands((ExpressionRewriteVisitor)v);
                }
                else if (v instanceof ExpressionVisitor) {
                    scan.visitComparands((ExpressionVisitor)v);
                }
            }
            TableGroupJoinNode next = root;
            top:
            while (true) {
                if (v.visitEnter(next.getTable())) {
                    TableGroupJoinNode node = next.getFirstChild();
                    if (node != null) {
                        next = node;
                        continue;
                    }
                }
                while (true) {
                    if (v.visitLeave(next.getTable())) {
                        TableGroupJoinNode node = next.getNextSibling();
                        if (node != null) {
                            next = node;
                            break;
                        }
                    }
                    if (next == root)
                        break top;
                    next = next.getParent();
                }
            }
        }
        return v.visitLeave(this);
    }

    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(group);
        str.append(", ");
        summarizeJoins(str);
        if (scan != null) {
            str.append(" - ");
            str.append(scan.summaryString());
        }
        str.append(")");
        return str.toString();
    }

    private void summarizeJoins(StringBuilder str) {
        for (TableGroupJoinNode node : this) {
            if (node != root) {
                str.append(" ");
                str.append(node.getParentJoinType());
                str.append(" ");
            }
            str.append(node.getTable().getName());
            if (node.getJoinConditions() != null) {
                boolean first = true;
                for (ConditionExpression joinCondition : node.getJoinConditions()) {
                    if (joinCondition.getImplementation() == ConditionExpression.Implementation.GROUP_JOIN)
                        continue;
                    if (first) {
                        str.append(" ON ");
                        first = false;
                    }
                    else
                        str.append(" AND ");
                    str.append(joinCondition);
                }
            }
        }
    }

    private <V> void buildLeaves(TableGroupJoinNode root, LeafFinderPredicate<V> predicate,
                                 Map<TableGroupJoinNode,V> out)
    {
        // parent caller is responsible for checking that this root satisfies the predicate
        TableGroupJoinNode child = root.getFirstChild();
        boolean hitLeaf;
        if (child == null) {
            hitLeaf = true;
        }
        else {
            boolean anyChildMatched = false;
            for (; child != null; child = child.getNextSibling()) {
                if (predicate.includeAndContinue(child, this)) {
                    buildLeaves(child, predicate, out);
                    anyChildMatched = true; // this frame's root is not the leaf of the predicate-matching tree
                }
            }
            hitLeaf = !anyChildMatched;
        }
        if (hitLeaf) {
            V value = predicate.mapToValue(root);
            out.put(root, value);
        }
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        throw new UnsupportedOperationException();
    }

}
