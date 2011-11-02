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

package com.akiban.server.test.it.sort;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.operator.UndefBindings;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.sort.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.persistit.exception.PersistitException;
import org.junit.Test;

public final class SortIT extends ITBase {
    @Test
    public void firstRowHasNulls() throws PersistitException {
        RowsBuilder rowsBuilder = new RowsBuilder(AkType.VARCHAR)
                .row(NullValueSource.only())
                .row("beta")
                .row("alpha");
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        PersistitAdapter adapter = new PersistitAdapter(schema, persistitStore(), treeService(), session(), akServer());
        TestOperator inputOperator = new TestOperator(rowsBuilder);

        Cursor inputCursor = API.cursor(inputOperator, adapter);
        inputCursor.open(UndefBindings.only());
        API.Ordering ordering = new API.Ordering();
        ordering.append(new FieldExpression(inputOperator.rowType(), 0), true);
        Sorter sorter = new Sorter(adapter, inputCursor, inputOperator.rowType(), ordering, UndefBindings.only());
        Cursor sortedCursor = sorter.sort();

        // check expected output
        Row[] expected = new RowsBuilder(AkType.VARCHAR)
                .row(NullValueSource.only())
                .row("alpha")
                .row("beta")
                .rows().toArray(new Row[3]);
        compareRows(expected, sortedCursor);
    }


}
