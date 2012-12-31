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

import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.Group;

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
        return new NodeIterator<T>(root);
    }
}
