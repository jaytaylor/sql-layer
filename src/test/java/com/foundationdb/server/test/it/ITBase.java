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
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.geophile.Space;
import com.foundationdb.server.test.ApiTestBase;
import com.foundationdb.server.test.it.qp.TestRow;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import org.junit.After;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class ITBase extends ApiTestBase {
    public ITBase() {
        super("IT");
    }

    protected ITBase(String suffix) {
        super(suffix);
    }

    protected static Row testRow(RowType type, Object... fields) {
        return new TestRow(type, fields);
    }

    protected static Collection<? extends BindableRow> bindableRows(Row... rows) {
        List<BindableRow> bindableRows = new ArrayList<>(rows.length);
        for(Row r : rows) {
            bindableRows.add(BindableRow.of(r));
        }
        return bindableRows;
    }

    protected void compareRows(Object[][] expected, Table table) {
        Schema schema = SchemaCache.globalSchema(ais());
        TableRowType rowType = schema.tableRowType(table);
        Row[] rows = new Row[expected.length];
        for (int i = 0; i < expected.length; i++) {
            rows[i] = new TestRow(rowType, expected[i]);
        }
        
        StoreAdapter adapter = newStoreAdapter(schema);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        
        List<TableRowType> keepTypes = Arrays.asList(rowType);
        
        compareRows (
                rows, 
                API.cursor(API.filter_Default(API.groupScan_Default(table.getGroup()),
                            keepTypes),
                        queryContext, 
                        queryContext.createBindings())
            );
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

    protected static void compareRows(Row[] expected, Row[] actual) {
        compareRows(Arrays.asList(expected), Arrays.asList(actual));
    }

    protected static void compareRows(Collection<? extends Row> expected, Collection<? extends Row> actual) {
        compareRows(expected, actual, false);
    }

    protected static void compareRows(Collection<? extends Row> expected, Collection<? extends Row> actual, boolean skipInternalColumns) {
        Iterator<? extends Row> eIt = expected.iterator();
        Iterator<? extends Row> aIt = actual.iterator();
        int i = 0;
        while(eIt.hasNext() && aIt.hasNext()) {
            compareTwoRows(eIt.next(), aIt.next(), i++, skipInternalColumns);
        }
        assertEquals("row count", expected.size(), actual.size());
    }

    protected void compareRows(Row[] expected, RowCursor cursor)
    {
        compareRows(expected, cursor, (cursor instanceof Cursor));
    }

    protected void compareRows(Row[] expected, RowCursor cursor, boolean topLevel) {
        boolean began = false;
        if(!txnService().isTransactionActive(session())) {
            txnService().beginTransaction(session());
            began = true;
        }
        boolean success = false;
        try {
            compareRowsInternal(expected, cursor, topLevel);
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

    private void compareRowsInternal(Row[] expected, RowCursor cursor, boolean topLevel)
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
                compareTwoRows(expected[count], actualRow, count);
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

    private static void compareTwoRows(Row expected, Row actual, int rowNumber) {
        compareTwoRows(expected, actual, rowNumber, false);
    }

    private static void compareTwoRows(Row expected, Row actual, int rowNumber, boolean skipInternalColumns) {
        if(!equal(expected, actual,skipInternalColumns)) {
            assertEquals("row " + rowNumber, String.valueOf(expected), String.valueOf(actual));
        }
        if(expected instanceof TestRow) {
            TestRow expectedTestRow = (TestRow) expected;
            if (expectedTestRow.persistityString() != null) {
                Object hKey = (actual != null) ? actual.hKey() : null;
                String actualHKeyString = String.valueOf(hKey);
                assertEquals(rowNumber + ": hkey", expectedTestRow.persistityString(), actualHKeyString);
            }
        }
    }

    private static boolean equal(Row expected, Row actual, boolean skipInternalColumns)
    {
        int nFields;
        if(skipInternalColumns){
            boolean equal = getTotalNonInternalColumns(expected) == getTotalNonInternalColumns(actual);
            if (!equal)
                return false;
            nFields = getTotalNonInternalColumns(actual);
        }//Used to ignore added pk column when create table as select is used
        else {
            boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
            if (!equal)
                return false;
           nFields = actual.rowType().nFields();
        }
        Space space = space(expected.rowType());
        if (space != null) {
            nFields = nFields - space.dimensions() + 1;
        }
        for (int actualPosition = 0, expectedPosition = 0;
                        actualPosition < nFields && expectedPosition < nFields;
                        actualPosition++, expectedPosition++) {
            if(skipInternalColumns) {
                while (isInternalColumn(actual, actualPosition)) {
                    if(++actualPosition == nFields)
                        return true;
                }
                while (isInternalColumn(expected, actualPosition)) {
                    if(++expectedPosition == nFields)
                        return true;
                }
            }
            ValueSource expectedField = expected.value(expectedPosition);
            ValueSource actualField = actual.value(actualPosition);
            TInstance expectedType = expected.rowType().typeAt(expectedPosition);
            TInstance actualType = actual.rowType().typeAt(actualPosition);
            assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));
            int c = TClass.compare(expectedType, expectedField, actualType, actualField);
            if (c != 0)
                return false;
        }
        return true;
    }

    private static int getTotalNonInternalColumns(Row row){
        int count = 0;
        for(int i = 0; i < row.rowType().nFields();i++){
            if(!isInternalColumn(row, i))
                count++;
        }
        return count;
    }

    private static boolean isInternalColumn(Row row, int position){
        if(row.rowType().hasTable()){
            return row.rowType().table().getColumnsIncludingInternal().get(position).isInternalColumn();
        }
        return false;
    }

    private static Space space(RowType rowType)
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

    public void lookForDanglingTrees() throws Exception {
        // Collect all trees storage currently has
        Set<String> storeTrees = new TreeSet<>();
        storeTrees.addAll(store().getStorageDescriptionNames());

        // Collect all trees in AIS
        Set<String> smTrees = serviceManager().getSchemaManager().getTreeNames(session());

        // Subtract knownTrees from storage trees instead of requiring exact. There may be allocated trees that
        // weren't materialized (yet), for example.
        Set<String> difference = new TreeSet<>(storeTrees);
        difference.removeAll(smTrees);

        assertEquals("Found orphaned trees", "[]", difference.toString());
    }
}
