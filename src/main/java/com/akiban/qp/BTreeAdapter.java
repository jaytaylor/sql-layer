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

package com.akiban.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.qp.rowtype.Schema;

public abstract class BTreeAdapter
{
    public abstract GroupCursor newGroupCursor(GroupTable groupTable);

    public abstract IndexCursor newIndexCursor(Index index);

    public final Schema schema()
    {
        return schema;
    }

    // For use by subclasses

    protected BTreeAdapter(Schema schema)
    {
        this.schema = schema;
    }

    // Object state

    protected final Schema schema;
}
