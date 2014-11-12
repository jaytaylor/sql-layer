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

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;

import java.util.Iterator;

public class TableSubTreeBase<T extends TableSubTreeBase.TableNodeBase<T>>
               implements Iterable<T>
{
    public static class TableNodeBase<T extends TableNodeBase<T>> {
        private Table table;
        private T parent, firstChild, nextSibling;

        public TableNodeBase(Table table) {
            this.table = table;
        }

        public Table getTable() {
            return table;
        }

        public Group getGroup() {
            return table.getGroup();
        }

        public int getNFields() {
            return table.getColumns().size();
        }

        public int getDepth() {
            return table.getDepth();
        }

        public int getOrdinal() {
            return table.getOrdinal();
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
            return table.getName().getTableName();
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
        return new NodeIterator<>(root);
    }
}
