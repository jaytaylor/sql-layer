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

public abstract class RowBase implements Row
{
    public abstract RowType rowType();

    public abstract <T> T field(int i);

    public abstract HKey hKey();

    public final boolean ancestorOf(Row that)
    {
        return this.hKey().prefixOf(that.hKey());
    }
}
