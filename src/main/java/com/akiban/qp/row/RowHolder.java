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

import com.akiban.util.ShareHolder;

public class RowHolder<MR extends Row>
{
    // Object interface

    @Override
    public String toString()
    {
        return holder.toString();
    }

    // RowHolder interface

    public MR get()
    {
        return holder.isHolding() ? holder.get() : null;
    }

    public void set(MR newRow)
    {
        if (newRow == null) {
            while (holder.isHolding()) {
                holder.release();
            }
        }
        else
            holder.hold(newRow);
    }

    public boolean isNull()
    {
        return ! holder.isHolding();
    }

    public boolean isNotNull()
    {
        return holder.isHolding();
    }

    public RowHolder(MR row)
    {
        set(row);
    }

    public RowHolder()
    {
    }

    // Object state

    private final ShareHolder<MR> holder = new ShareHolder<MR>();
}
