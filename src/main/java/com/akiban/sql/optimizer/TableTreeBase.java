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

package com.akiban.sql.optimizer;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class TableTreeBase<T extends TableTreeBase.TableNodeBase<T>> 
                implements Iterable<T>
{
    public static class TableNodeBase<T extends TableNodeBase<T>> {
        private UserTable table;
        private T parent, firstChild, nextSibling;

        public TableNodeBase(UserTable table) {
            this.table = table;
        }

        public UserTable getTable() {
            return table;
        }

        public Group getGroup() {
            return table.getGroup();
        }

        public GroupTable getGroupTable() {
            return table.getGroup().getGroupTable();
        }

        public int getNFields() {
            return table.getColumns().size();
        }

        public int getDepth() {
            return table.getDepth();
        }

        /** Is <code>this</code> an ancestor of <code>other</code>? */
        public boolean isAncestor(T other) {
            if (getDepth() >= other.getDepth()) 
                return false;
            while (true) {
                if (other == this)
                    return true;
                other = other.getParent();
                if (other == null)
                    return false;
            }
        }

        public T getParent() {
            return parent;
        }

        public T getFirstChild() {
            return firstChild;
        }

        public T getNextSibling() {
            return nextSibling;
        }

        public void setParent(T parent) {
            this.parent = parent;
        }

        public void setFirstChild(T firstChild) {
            this.firstChild = firstChild;
        }

        public void setNextSibling(T nextSibling) {
            this.nextSibling = nextSibling;
        }

        public String toString() {
            return table.toString();
        }
    }

    private T root;
    private Map<UserTable,T> map = new HashMap<UserTable,T>();

    public T getRoot() {
        return root;
    }

    public T getNode(UserTable table) {
        return map.get(table);
    }

    protected abstract T createNode(UserTable table) throws StandardException;

    public T addNode(UserTable table) throws StandardException {
        return addNode(table, false);
    }

    public T addNode(UserTable table, boolean useExisting) throws StandardException {
        T node = getNode(table);
        if (node != null) {
            if (!useExisting)
                throw new StandardException("Table " + table + " already present");
            return node;
        }
        node = createNode(table);
        map.put(table, node);
        if (root == null) {
            root = node;
            return node;
        }
        T toInsert = node;
        if (node.getDepth() < root.getDepth()) {
            toInsert = root;
            root = node;
        }
        UserTable parentTable = ((UserTable)toInsert.getTable()).parentTable();
        assert (parentTable != null);
        T parent = addNode(parentTable, true);
        assert ((toInsert.getParent() == null) && // Brand new or old root.
                (toInsert.getNextSibling() == null));
        toInsert.setParent(parent);
        toInsert.setNextSibling(parent.getFirstChild());
        parent.setFirstChild(toInsert);
        return node;
    }

    static class NodeIterator<T extends TableNodeBase<T>> implements Iterator<T> {
        private T next;

        NodeIterator(T root) {
            next = root;
        }

        public boolean hasNext() {
            return (next != null);
        }

        public T next() {
            T onext = next;
            next = onext.getFirstChild();
            if (next == null) {
                T node = onext;
                while (true) {
                    next = node.getNextSibling();
                    if (next != null) break;
                    node = node.getParent();
                    if (node == null) break;
                }
            }
            return onext;
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
    
    public Iterator<T> iterator() {
        return new NodeIterator<T>(root);
    }
}
