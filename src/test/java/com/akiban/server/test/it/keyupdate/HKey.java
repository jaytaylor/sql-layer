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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowDef;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;

public class HKey implements Comparable<HKey>
{
    // Object interface

    @Override
    public int hashCode()
    {
        int h = 0;
        for (Object element : elements) {
            h = h * 9987001 + (element instanceof UserTable ? ((UserTable) element).getTableId() : element.hashCode());
        }
        return h;
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof HKey;
        if (eq) {
            HKey that = (HKey) o;
            eq = this.elements.size() == that.elements.size();
            int i = 0;
            while (eq && i < elements.size()) {
                Object thisElement = this.elements.get(i);
                Object thatElement = that.elements.get(i);
                eq = thisElement == thatElement // Takes care of UserTables
                     || thisElement.equals(thatElement);
                i++;
            }
        }
        return eq;
    }

    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        boolean first = true;
        for (Object element : elements) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            if (element instanceof UserTable) {
                UserTable table = (UserTable) element;
                buffer.append(table.getName().getTableName());
            } else {
                buffer.append(element);
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    // Comparable interface

    @Override
    public int compareTo(HKey that)
    {
        int c = 0;
        Iterator<Object> i = this.elements.iterator();
        Iterator<Object> j = that.elements.iterator();
        while (c == 0 && i.hasNext() && j.hasNext()) {
            Object iElement = i.next();
            Object jElement = j.next();
            if (iElement == null && jElement == null) {
                // c is already 0
            } else if (iElement == null) {
                c = -1;
            } else if (jElement == null) {
                c = 1;
            } else {
                assertSame(iElement.getClass(), jElement.getClass());
                if (iElement instanceof UserTable) {
                    c = ((UserTable) iElement).getTableId() - ((UserTable) jElement).getTableId();
                } else {
                    c = ((Comparable)iElement).compareTo(jElement);
                }
            }
        }
        if (c == 0) {
            assertTrue((i.hasNext() ? 1 : 0) + (j.hasNext() ? 1 : 0) <= 1);
            c = i.hasNext() ? 1 : j.hasNext() ? -1 : 0;
        }
        return c;
    }


    // HKey interface

    public Object[] objectArray()
    {
        return elements.toArray();
    }

    public HKey copy()
    {
        return new HKey(elements.toArray());
    }

    public HKey(Object... elements)
    {
        this.elements = new ArrayList<Object>();
        for (Object element : elements) {
            if (element instanceof RowDef) {
                element = ((RowDef) element).userTable();
            }
            this.elements.add(element);
        }
    }

    // Object state

    private List<Object> elements;
}
