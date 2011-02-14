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

import com.akiban.server.api.dml.scan.NiceRow;

public class TestRow extends NiceRow
{
    public TestRow(int tableId)
    {
        super(tableId);
    }

    public HKey hKey()
    {
        return hKey;
    }

    public void hKey(HKey hKey)
    {
        this.hKey = hKey;
    }

    public TestRow parent()
    {
        return parent;
    }

    public void parent(TestRow parent)
    {
        this.parent = parent;
    }

    private HKey hKey;
    private TestRow parent;
}
