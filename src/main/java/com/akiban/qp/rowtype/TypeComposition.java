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

package com.akiban.qp.rowtype;


import com.akiban.ais.model.UserTable;
import com.akiban.util.ArgumentValidation;

import java.util.*;

public class TypeComposition
{
    public boolean isAncestorOf(TypeComposition that)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isParentOf(TypeComposition that)
    {
        throw new UnsupportedOperationException();
    }

    public final Set<UserTable> tables()
    {
        return tables;
    }

    public TypeComposition(RowType rowType, Collection<UserTable> tables)
    {
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notEmpty("tables", tables);
        this.rowType = rowType;
        this.tables = Collections.unmodifiableSet(new HashSet<UserTable>(tables));
    }

    // Object state

    protected final RowType rowType;
    protected final Set<UserTable> tables;
}
