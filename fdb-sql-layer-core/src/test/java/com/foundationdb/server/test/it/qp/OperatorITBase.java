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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.*;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.Strings;

import com.geophile.z.SpatialObject;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.foundationdb.qp.operator.API.cursor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OperatorITBase extends ITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(OperatorITBase.class.getName());

    /** Override if derived IT manages its own transaction */
    protected boolean doAutoTransaction() {
        return true;
    }

    @Before
    public final void runAllSetup() {
        setupCreateSchema();
        if(doAutoTransaction()) {
            txnService().beginTransaction(session());
        }
        schema = SchemaCache.globalSchema(ais());
        assert schema != null : "no schema in ais";
        adapter = newStoreAdapter();
        setupPostCreateSchema();
    }


    @After
    public final void after_endTransaction() {
        if(doAutoTransaction()) {
            txnService().commitTransaction(session());
        }
    }

    protected void setupCreateSchema() {
        customer = createTable(
                "schema", "customer",
                "cid int not null primary key",
                "name varchar(20)");
        createIndex("schema", "customer", "name", "name");
        order = createTable(
                "schema", "order",
                "oid int not null primary key",
                "cid int",
                "salesman varchar(20)",
                "grouping foreign key (cid) references customer(cid)");
        createIndex("schema", "order", "salesman", "salesman");
        createIndex("schema", "order", "cid", "cid");
        item = createTable(
                "schema", "item",
                "iid int not null primary key",
                "oid int",
                "grouping foreign key (oid) references \"order\"(oid)");
        createIndex("schema", "item", "oid", "oid");
        createIndex("schema", "item", "oid2", "oid", "iid");
        address = createTable(
                "schema", "address",
                "aid int not null primary key",
                "cid int",
                "address varchar(100)",
                "grouping foreign key (cid) references customer(cid)");
        createIndex("schema", "address", "cid", "cid");
        createIndex("schema", "address", "address", "address");
        createLeftGroupIndex(new TableName("schema", "customer"), "cname_ioid", "customer.name", "item.oid");
    }

    protected void setupPostCreateSchema() {
        customerRowType = schema.tableRowType(table(customer));
        orderRowType = schema.tableRowType(table(order));
        itemRowType = schema.tableRowType(table(item));
        addressRowType = schema.tableRowType(table(address));
        orderHKeyRowType = schema.newHKeyRowType(orderRowType.hKey());
        customerNameIndexRowType = indexType(customer, "name");
        orderSalesmanIndexRowType = indexType(order, "salesman");
        orderCidIndexRowType = indexType(order, "cid");
        itemOidIndexRowType = indexType(item, "oid");
        itemOidIidIndexRowType = indexType(item, "oid", "iid");
        itemIidIndexRowType = indexType(item, "iid");
        customerCidIndexRowType = indexType(customer, "cid");
        addressCidIndexRowType = indexType(address, "cid");
        addressAddressIndexRowType = indexType(address, "address");
        customerNameItemOidIndexRowType = groupIndexType(Index.JoinType.LEFT, "customer.name", "item.oid");
        coi = group(customer);
        customerOrdinal =  ddl().getTable(session(),  customer).getOrdinal();
        orderOrdinal =  ddl().getTable(session(),  order).getOrdinal();
        itemOrdinal = ddl().getTable(session(),  item).getOrdinal();
        addressOrdinal =  ddl().getTable(session(),  address).getOrdinal();
        db = new Row[]{ row(customer, 1L, "xyz"),
                        row(customer, 2L, "abc"),
                        row(order, 11L, 1L, "ori"),
                        row(order, 12L, 1L, "david"),
                        row(order, 21L, 2L, "tom"),
                        row(order, 22L, 2L, "jack"),
                        row(item, 111L, 11L),
                        row(item, 112L, 11L),
                        row(item, 121L, 12L),
                        row(item, 122L, 12L),
                        row(item, 211L, 21L),
                        row(item, 212L, 21L),
                        row(item, 221L, 22L),
                        row(item, 222L, 22L)};
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void testCursorLifecycle(Operator scan, CursorLifecycleTestCase testCase, AkCollator ... collators)
    {
        Cursor cursor = cursor(scan, queryContext, queryBindings);
        cursor.openBindings();
        cursor.nextBindings();
        // Check idle following creation
        assertTrue(cursor.isClosed());
        // Check active following open
        testCase.firstSetup();
        cursor.open();
        assertTrue(cursor.isActive());
        // Check idle following close
        cursor.close();
        assertTrue(cursor.isClosed());
        // Check active following re-open
        testCase.firstSetup();
        cursor.open();
        assertTrue(cursor.isActive());
        cursor.close();
        // Check active during iteration
        testCase.firstSetup();
        if (testCase.hKeyComparison()) {
            compareRenderedHKeys(testCase.firstExpectedHKeys(), cursor, testCase.reopenTopLevel());
        } else {
            compareRows(testCase.firstExpectedRows(), cursor, testCase.reopenTopLevel());
        }
        assertTrue(cursor.isClosed());
        // Check close during iteration.
        if (testCase.hKeyComparison()
            ? testCase.firstExpectedHKeys().length > 1
            : testCase.firstExpectedRows().length > 1) {
            testCase.firstSetup();
            if (testCase.reopenTopLevel())
                cursor.openTopLevel();
            else
                cursor.open();
            cursor.next();
            assertTrue(cursor.isActive());
            cursor.close();
            assertTrue(cursor.isClosed());
        }
        // Check that a second execution works
        testCase.secondSetup();
        if (testCase.hKeyComparison()) {
            compareRenderedHKeys(testCase.secondExpectedHKeys(), cursor, testCase.reopenTopLevel());
        } else {
            compareRows(testCase.secondExpectedRows(), cursor, testCase.reopenTopLevel());
        }
        assertTrue(cursor.isClosed());
    }

    protected void use(Row[] db)
    {
        writeRows(db);
    }

    protected Group group(int tableId)
    {
        return ais().getTable(tableId).getGroup();
    }

    protected Table table(int tableId)
    {
        return ais().getTable(tableId);
    }

    protected IndexRowType indexType(int tableId, String... columnNamesArray)
    {
        List<String> searchIndexColumnNames = Arrays.asList(columnNamesArray);
        Table table = table(tableId);
        for (Index index : table.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.tableRowType(table(tableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected IndexRowType groupIndexType(TableName groupName, String... columnNamesArray)
    {
        List<String> searchIndexColumnNames = Arrays.asList(columnNamesArray);
        for (Index index : ais().getGroup(groupName).getIndexes()) {
            List<String> indexColumnNames = new ArrayList<>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                Column column = indexColumn.getColumn();
                indexColumnNames.add(String.format("%s.%s",
                                                   column.getTable().getName().getTableName(), column.getName()));
            }
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.indexRowType(index);
            }
        }
        return null;
    }

    protected IndexRowType groupIndexType(Index.JoinType joinType, String ... columnNames)
    {
        IndexRowType selectedGroupIndexRowType = null;
        for (IndexRowType groupIndexRowType : schema.groupIndexRowTypes()) {
            boolean match = groupIndexRowType.index().getJoinType() == joinType;
            for (IndexColumn indexColumn : groupIndexRowType.index().getKeyColumns()) {
                Column column = indexColumn.getColumn();
                String indexColumnName = String.format("%s.%s", column.getTable().getName().getTableName(),
                                                       column.getName());
                // Why do index column names lose case?!
                if (indexColumn.getPosition() < columnNames.length &&
                    !columnNames[indexColumn.getPosition()].equalsIgnoreCase(indexColumnName)) {
                    match = false;
                }
            }
            if (match) {
                selectedGroupIndexRowType = groupIndexRowType;
            }
        }
        return selectedGroupIndexRowType;
    }

    protected ColumnSelector columnSelector(final Index index)
    {
        final int columnCount = index.getKeyColumns().size();
        return new ColumnSelector() {
            @Override
            public boolean includesColumn(int columnPosition) {
                return columnPosition < columnCount;
            }
        };
    }

    protected TestRow row(String hKeyString, RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields, hKeyString);
    }

    protected Row row(IndexRowType indexRowType, Object... objs) {
/*
        try {
*/
            ValuesHolderRow row = new ValuesHolderRow(indexRowType);
            for (int i = 0; i < objs.length; i++) {
                Object obj = objs[i];
                Value value = row.valueAt(i);
                if (obj == null) {
                    value.putNull();
                } else if (obj instanceof Integer) {
                    if (ValueSources.underlyingType(value) == UnderlyingType.INT_64)
                        value.putInt64(((Integer) obj).longValue());
                    else
                        value.putInt32((Integer) obj);
                } else if (obj instanceof Long) {
                    if (ValueSources.underlyingType(value) == UnderlyingType.INT_32)
                        value.putInt32(((Long) obj).intValue());
                    else
                        value.putInt64((Long) obj);
                } else if (obj instanceof String) {
                    value.putString((String) obj, null);
                } else if (obj instanceof BigDecimal) {
                    value.putObject(new BigDecimalWrapperImpl((BigDecimal) obj));
                } else if (obj instanceof SpatialObject) {
                    value.putObject(obj);
                } else {
                    fail(obj.toString());
                }
            }
            return row;
/*
            return new PersistitIndexRow(adapter, indexRowType, objs);
        } catch(PersistitException e) {
            throw new RuntimeException(e);
        }
*/
    }

    protected String hKeyValue(Long x)
    {
        return x == null ? "null" : String.format("(long)%d", x);
    }

    // Useful when scanning is expected to throw an exception
    protected void scan(Cursor cursor)
    {
        List<Row> actualRows = new ArrayList<>(); // So that result is viewable in debugger
        try {
            cursor.openTopLevel();
            Row actualRow;
            while ((actualRow = cursor.next()) != null) {
                actualRows.add(actualRow);
            }
        } finally {
            cursor.closeTopLevel();
        }
    }

    @SuppressWarnings("unused") // useful for debugging
    protected void dumpToAssertion(Cursor cursor)
    {
        List<String> strings = new ArrayList<>();
        try {
            cursor.openTopLevel();
            Row row;
            while ((row = cursor.next()) != null) {
                strings.add(String.valueOf(row));
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            cursor.closeTopLevel();
        }
        strings.add(0, strings.size() == 1 ? "1 string:" : strings.size() + " strings:");
        throw new AssertionError(Strings.join(strings));
    }

    @SuppressWarnings("unused") // useful for debugging
    protected void dumpToAssertion(Operator plan)
    {
        dumpToAssertion(cursor(plan, queryContext, queryBindings));
    }

    protected void dump(Cursor cursor)
    {
        cursor.openTopLevel();
        Row row;
        while ((row = cursor.next()) != null) {
            LOG.debug("{}", String.valueOf(row));
        }
        cursor.closeTopLevel();
    }
    
    protected void dump(Operator plan)
    {
        dump(cursor(plan, queryContext, queryBindings));
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        compareRenderedHKeys(expected, cursor, true);
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor, boolean topLevel)
    {
        int count;
        try {
            if (topLevel)
                cursor.openTopLevel();
            else
                cursor.open();
            count = 0;
            List<Row> actualRows = new ArrayList<>(); // So that result is viewable in debugger
            Row actualRow;
            while ((actualRow = cursor.next()) != null) {
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            if (topLevel)
                cursor.closeTopLevel();
            else
                cursor.close();
        }
        assertEquals(expected.length, count);
    }

    protected int ordinal(RowType rowType)
    {
        return rowType.table().getOrdinal();
    }


    public Operator rowsToValueScan(Row... rows) {
        List<BindableRow> bindableRows = new ArrayList<>();
        RowType type = null;
        for(Row row : rows) {
            RowType newType = row.rowType();
            if(type == null) {
                type = newType;
            } else if(type != newType) {
                fail("Multiple row types: " + type + " vs " + newType);
            }
            bindableRows.add(BindableRow.of(row));
        }
        return API.valuesScan_Default(bindableRows, type);
    }


    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected TableRowType customerRowType;
    protected TableRowType orderRowType;
    protected TableRowType itemRowType;
    protected TableRowType addressRowType;
    protected HKeyRowType orderHKeyRowType;
    protected IndexRowType customerCidIndexRowType;
    protected IndexRowType customerNameIndexRowType;
    protected IndexRowType orderSalesmanIndexRowType;
    protected IndexRowType orderCidIndexRowType;
    protected IndexRowType itemOidIndexRowType;
    protected IndexRowType itemOidIidIndexRowType;
    protected IndexRowType itemIidIndexRowType;
    protected IndexRowType addressCidIndexRowType;
    protected IndexRowType addressAddressIndexRowType;
    protected IndexRowType customerNameItemOidIndexRowType;
    protected Group coi;
    protected Schema schema;
    protected Row[] db;
    protected Row[] emptyDB = new Row[0];
    protected StoreAdapter adapter;
    protected QueryBindings queryBindings;
    protected QueryContext queryContext;
    protected AkCollator ciCollator;
    protected int customerOrdinal;
    protected int orderOrdinal;
    protected int itemOrdinal;
    protected int addressOrdinal;

    protected static abstract class CursorLifecycleTestCase
    {
        public boolean hKeyComparison()
        {
            return false;
        }

        public void firstSetup()
        {}

        public Row[] firstExpectedRows()
        {
            fail();
            return null;
        }

        public String[] firstExpectedHKeys()
        {
            fail();
            return null;
        }

        public void secondSetup()
        {}

        public Row[] secondExpectedRows()
        {
            return firstExpectedRows();
        }

        public String[] secondExpectedHKeys()
        {
            return firstExpectedHKeys();
        }

        public boolean reopenTopLevel() {
            return false;
        }
    }
}
