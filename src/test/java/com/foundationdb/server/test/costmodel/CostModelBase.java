/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.util.tap.Tap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.Math.abs;

public class CostModelBase extends ApiTestBase
{
    protected CostModelBase()
    {
        super("CT");
        disableTaps();
    }

    protected Group group(int userTableId)
    {
        return getRowDef(userTableId).table().getGroup();
    }

    protected UserTable userTable(int userTableId)
    {
        return getRowDef(userTableId).userTable();
    }

    protected IndexRowType indexType(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.userTableRowType(userTable(userTableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected ColumnSelector columnSelector(final Index index)
    {
        return new ColumnSelector()
        {
            @Override
            public boolean includesColumn(int columnPosition)
            {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    if (column.getPosition() == columnPosition) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    protected RowBase row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    protected String schemaName()
    {
        return "schema";
    }
    
    protected String newTableName()
    {
        return String.format("t%s", abs(System.nanoTime()));
    }

    private static void disableTaps()
    {
        Tap.setEnabled(".*", false);
    }

    protected Schema schema;
    protected StoreAdapter adapter;
    protected QueryContext queryContext;
    protected QueryBindings queryBindings;
}
