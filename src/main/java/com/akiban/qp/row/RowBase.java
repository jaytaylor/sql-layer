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

package com.akiban.qp.row;

import com.akiban.qp.HKey;
import com.akiban.qp.rowtype.RowType;

public abstract class RowBase implements ManagedRow
{
    // Row interface

    @Override
    public abstract RowType rowType();

    @Override
    public abstract <T> T field(int i);

    @Override
    public abstract HKey hKey();

    @Override
    public final boolean ancestorOf(Row that)
    {
        return this.hKey().prefixOf(that.hKey());
    }

    @Override
    public final ManagedRow managedRow()
    {
        return this;
    }

    // ManagedRow interface

    @Override
    public final void share()
    {
        assert references >= 0 : this;
        references++;
        // System.out.println(String.format("%s: share %s", references, this));
    }

    @Override
    public final boolean isShared()
    {
        assert references >= 1 : this;
        // System.out.println(String.format("%s: isShared %s", references, this));
        return references > 1;
    }

    @Override
    public final void release()
    {
        assert references >= 1 : this;
        references--;
        // System.out.println(String.format("%s: release %s", references, this));
    }

    // Object state

    private int references = 0;
}
