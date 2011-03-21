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

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class UserTableRowType implements RowType
{
    // RowType interface

    @Override
    public boolean ancestorOf(RowType type)
    {
        assert type instanceof UserTableRowType : type;
        // TODO: Something faster
        UserTable ancestor = ((UserTableRowType)type).table;
        do {
            Join join = ancestor.getParentJoin();
            ancestor = join == null ? null : join.getParent();
        } while (ancestor != table && ancestor != null);
        return ancestor != null;
    }

    // UserTableRowType interface

    public UserTableRowType(UserTable table)
    {
        this.table = table;
    }

    // Object state

    private UserTable table;
}
