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

package com.foundationdb.ais.model;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKey
{
    public List<Column> getColumns()
    {
        if (columns == null) {
            columns = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                columns.add(indexColumn.getColumn());
            }
        }
        return columns;
    }

    public TableIndex getIndex()
    {
        return index;
    }

    /** Indicates whether this primary key was generated and is not part of the declared schema. */
    public boolean isAkibanPK()
    {
        return getColumns().size() == 1 && getColumns().get(0).isAkibanPKColumn();
    }

    public PrimaryKey(TableIndex index)
    {
        this.index = index;
    }

    private final TableIndex index;
    private List<Column> columns;
}
