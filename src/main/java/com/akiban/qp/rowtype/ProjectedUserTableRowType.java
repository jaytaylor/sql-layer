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

import java.util.List;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.expression.Expression;

public class ProjectedUserTableRowType extends ProjectedRowType {

    public ProjectedUserTableRowType(DerivedTypesSchema schema, UserTable table, List<? extends Expression> projections) {
        super(schema, table.getTableId(), projections);
        this.table = table;
    }

    @Override
    public UserTable userTable() {
        return table;
    }

    @Override
    public boolean hasUserTable() {
        return table != null;
    }
    
    @Override
    public int nFields()
    {
        return table.getColumns().size();
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    @Override
    public String toString()
    {
        return String.format("%s: %s", super.toString(), table);
    }

    private final UserTable table;
    

}
