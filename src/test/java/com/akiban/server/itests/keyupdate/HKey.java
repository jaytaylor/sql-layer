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

package com.akiban.server.itests.keyupdate;

import com.akiban.ais.model.UserTable;
import com.akiban.server.RowDef;

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
