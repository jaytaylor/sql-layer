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

import com.akiban.server.rowdata.RowDef;

import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupTable;

import java.util.Iterator;

public class TableSubTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
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

        public int getOrdinal() {
            return table.rowDef().getOrdinal();
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

        public TableSubTreeBase<T> subtree() {
            return new TableSubTreeBase(this);
        }

        public String toString() {
            return table.toString();
        }
    }

    static class NodeIterator<T extends TableNodeBase<T>> implements Iterator<T> {
        private T root, next;

        NodeIterator(T root) {
            this.next = this.root = root;
        }

        public boolean hasNext() {
            return (next != null);
        }

        public T next() {
            T onext = next;
            next = onext.getFirstChild();
            if (next == null) {
                T node = onext;
                while (node != root) {
                    next = node.getNextSibling();
                    if (next != null) break;
                    node = node.getParent();
                }
            }
            return onext;
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    protected T root;

    protected TableSubTreeBase(T root) {
        this.root = root;
    }

    public T getRoot() {
        return root;
    }
    
    public Iterator<T> iterator() {
        return new NodeIterator<T>(root);
    }
}
