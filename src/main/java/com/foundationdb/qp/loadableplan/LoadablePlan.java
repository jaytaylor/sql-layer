/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.loadableplan;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;

import java.util.ArrayList;
import java.util.List;

public abstract class LoadablePlan<T>
{
    public abstract T plan();

    public abstract int[] jdbcTypes();

    public List<String> columnNames()
    {
        List<String> columnNames = new ArrayList<>();
        int columns = jdbcTypes().length;
        for (int c = 0; c < columns; c++) {
            columnNames.add(String.format("c%d", c));
        }
        return columnNames;
    }

    public final void ais(AkibanInformationSchema ais)
    {
        this.ais = ais;
    }

    public final AkibanInformationSchema ais()
    {
        return ais;
    }

    public Schema schema()
    {
        return SchemaCache.globalSchema(ais);
    }

    private AkibanInformationSchema ais;
}
