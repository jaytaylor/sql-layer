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

package com.akiban.qp.loadableplan;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.sql.pg.PostgresType;
import com.akiban.sql.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.List;

public abstract class LoadablePlan<T>
{
    public abstract String name();

    public abstract T plan();

    public abstract int[] jdbcTypes();

    public List<String> columnNames()
    {
        List<String> columnNames = new ArrayList<String>();
        int columns = jdbcTypes().length;
        for (int c = 0; c < columns; c++) {
            columnNames.add(String.format("c%d", c));
        }
        return columnNames;
    }

    public final List<PostgresType> columnTypes()
    {
        List<PostgresType> columnTypes = new ArrayList<PostgresType>();
        for (int jdbcType : jdbcTypes()) {
            DataTypeDescriptor d = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType);
            columnTypes.add(PostgresType.fromDerby(d));
        }
        return columnTypes;
    }

    public final void ais(AkibanInformationSchema ais)
    {
        this.ais = ais;
    }

    protected AkibanInformationSchema ais()
    {
        return ais;
    }

    protected Schema schema()
    {
        return SchemaCache.globalSchema(ais);
    }

    private AkibanInformationSchema ais;
}
