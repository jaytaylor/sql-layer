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

package com.foundationdb.server.test.it;

import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class ITBase extends ApiTestBase {
    public ITBase() {
        super("IT");
    }

    protected ITBase(String suffix) {
        super(suffix);
    }

    protected void compareRows(Object[][] expected, Index index) {
        Schema schema = SchemaCache.globalSchema(ais());
        IndexRowType rowType = schema.indexRowType(index);
        Row[] rows = new Row[expected.length];
        for(int i = 0; i < expected.length; ++i) {
            rows[i] = new TestRow(rowType, expected[i]);
        }
        StoreAdapter adapter = newStoreAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        compareRows(
            rows,
            API.cursor(
                API.indexScan_Default(rowType, false, IndexKeyRange.unbounded(rowType)),
                queryContext,
                queryContext.createBindings()
            )
        );
    }

    protected void compareRows(Row[] expected, RowCursor cursor)
    {
        compareRows(expected, cursor, (cursor instanceof Cursor), null);
    }

    protected void compareRows(Row[] expected, RowCursor cursor, AkCollator ... collators)
    {
        compareRows(expected, cursor, (cursor instanceof Cursor), collators);
    }

    protected void compareRows(Row[] expected, RowCursor cursor, boolean topLevel, AkCollator ... collators) {
        boolean began = false;
        if(!txnService().isTransactionActive(session())) {
            txnService().beginTransaction(session());
            began = true;
        }
        boolean success = false;
        try {
            compareRowsInternal(expected, cursor, topLevel, collators);
            success = true;
        } finally {
            if(began) {
                if(success) {
                    txnService().commitTransaction(session());
                } else {
                    txnService().rollbackTransaction(session());
                }
            }
        }
    }

    private void compareRowsInternal(Row[] expected, RowCursor cursor, boolean topLevel, AkCollator ... collators)
    {
        List<Row> actualRows = new ArrayList<>(); // So that result is viewable in debugger
        try {
            if (topLevel)
                ((Cursor)cursor).openTopLevel();
            else
                cursor.open();
            Row actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(String.format("failed test %d < %d (more rows than expected)", count, expected.length), count < expected.length);
                if(!equal(expected[count], actualRow, collators)) {
                    String expectedString = expected[count] == null ? "null" : expected[count].toString();
                    String actualString = actualRow == null ? "null" : actualRow.toString();
                    assertEquals("row " + count, expectedString, actualString);
                }
                if (expected[count] instanceof TestRow) {
                    TestRow expectedTestRow = (TestRow) expected[count];
                    if (expectedTestRow.persistityString() != null) {
                        String actualHKeyString = actualRow == null ? "null" : actualRow.hKey().toString();
                        assertEquals(count + ": hkey", expectedTestRow.persistityString(), actualHKeyString);
                    }
                }
                actualRows.add(actualRow);
            }
        } finally {
            if (topLevel)
                ((Cursor)cursor).closeTopLevel();
            else
                cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    private boolean equal(Row expected, Row actual, AkCollator[] collators)
    {
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        if (!equal)
            return false;
        int nFields = actual.rowType().nFields();
        Space space = space(expected.rowType());
        if (space != null) {
            nFields = nFields - space.dimensions() + 1;
        }
        for (int i = 0; i < nFields; i++) {
            ValueSource expectedField = expected.value(i);
            ValueSource actualField = actual.value(i);
            TInstance expectedType = expected.rowType().typeInstanceAt(i);
            TInstance actualType = actual.rowType().typeInstanceAt(i);
            assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));
            int c = TClass.compare(expectedType, expectedField, actualType, actualField);
            if (c != 0)
                return false;
        }
        return true;
    }

    private Space space(RowType rowType)
    {
        Space space = null;
        if (rowType instanceof IndexRowType) {
            Index index = ((IndexRowType)rowType).index();
            if (index.isSpatial()) {
                space = index.space();
            }
        }
        return space;
    }

    private AkCollator collator(AkCollator[] collators, int i)
    {
        return
            collators == null ? null :
            collators.length == 0 ? null : collators[i];
    }
}
