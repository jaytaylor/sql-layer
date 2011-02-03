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

public class TreeRecord
{
    @Override
    public int hashCode()
    {
        return hKey.hashCode() ^ row.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        boolean eq = o != null && o instanceof TreeRecord;
        if (eq) {
            TreeRecord that = (TreeRecord) o;
            eq = this.hKey.equals(that.hKey) && equals(this.row, that.row);
        }
        return eq;
    }

    @Override
    public String toString()
    {
        return String.format("%s -> %s", hKey, row);
    }

    public HKey hKey()
    {
        return hKey;
    }

    public NewRow row()
    {
        return row;
    }

    public TreeRecord(HKey hKey, NewRow row)
    {
        this.hKey = hKey;
        this.row = row;
    }

    public TreeRecord(Object[] hKey, NewRow row)
    {
        this(new HKey(hKey), row);
    }

    private boolean equals(NewRow x, NewRow y)
    {
        return
            x == y ||
            x != null && y != null && x.getRowDef() == y.getRowDef() && x.getFields().equals(y.getFields());
    }

    private final HKey hKey;
    private final NewRow row;
}
