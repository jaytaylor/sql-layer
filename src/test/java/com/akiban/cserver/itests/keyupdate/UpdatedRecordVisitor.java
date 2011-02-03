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

package com.akiban.cserver.itests.keyupdate;

import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.store.TreeRecordVisitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertTrue;

class UpdatedRecordVisitor extends KeyUpdateTestRecordVisistor
{
    @Override
    public void visit(Object[] key, NewRow row)
    {
        boolean checked = false;
        for (Iterator<Update> i = updates.iterator(); !checked && i.hasNext();) {
            Update update = i.next();
            if (equal(update.key, key)) {
                assertTrue(equal(update.row, row));
                checked = true;
            }
        }
        if (!checked) {
            visitor.visit(key, row);
        }
    }

    public void addUpdate(Object[] key, NewRow row)
    {
        updates.add(new Update(key, row));
    }

    public UpdatedRecordVisitor(TreeRecordVisitor visitor)
    {
        this.visitor = visitor;
    }

    private boolean equal(Object[] x, Object[] y)
    {
        boolean equal = x.length == y.length;
        for (int i = 0; equal && i < x.length; i++) {
            equal = x[i] == y[i];
        }
        return equal;
    }

    private boolean equal(NewRow x, NewRow y)
    {
        boolean equal = x.getRowDef() == y.getRowDef();
        Set<Integer> keys = x.getFields().keySet();
        for (Iterator<Integer> k = keys.iterator(); equal && k.hasNext();) {
            Integer key = k.next();
            equal = equal(x.get(key), y.get(key));
        }
        return equal;
    }

    private boolean equal(Object x, Object y)
    {
        return
            x == y ? true :
            x == null ? false :
            y == null ? false :
            x.equals(y);
    }

    private final TreeRecordVisitor visitor;
    private final List<Update> updates = new ArrayList<Update>();

    private static class Update
    {
        // row is null for a deletion
        public Update(Object[] key, NewRow row)
        {
            this.key = key;
            this.row = row;
        }

        public Object[] key;
        public NewRow row;
    }
}