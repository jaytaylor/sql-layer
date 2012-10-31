/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.*;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.row.RowValuesHolder;
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.*;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimalWrapper;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.persistit.exception.PersistitException;
import com.akiban.util.Strings;
import org.junit.After;
import org.junit.Before;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.akiban.qp.operator.API.cursor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OperatorITBase extends ITBase
{
    @Before
    public void before_beginTransaction() throws PersistitException {
        txnService().beginTransaction(session());
    }

    @After
    public void after_endTransaction() throws PersistitException {
        txnService().commitTransaction(session());
        txnService().rollbackTransactionIfOpen(session());
    }

    @Before
    public final void runAllSetup() {
        setupCreateSchema();
        setupPostCreateSchema();
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
        createGroupIndex("customer", "cname_ioid", "customer.name,item.oid", Index.JoinType.LEFT);
    }

    protected void setupPostCreateSchema() {
        schema = new Schema(ais());
        customerRowType = schema.userTableRowType(userTable(customer));
        orderRowType = schema.userTableRowType(userTable(order));
        itemRowType = schema.userTableRowType(userTable(item));
        addressRowType = schema.userTableRowType(userTable(address));
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
        customerOrdinal =  ddl().getTable(session(),  customer).rowDef().getOrdinal();
        orderOrdinal =  ddl().getTable(session(),  order).rowDef().getOrdinal();
        itemOrdinal = ddl().getTable(session(),  item).rowDef().getOrdinal();
        addressOrdinal =  ddl().getTable(session(),  address).rowDef().getOrdinal();
        db = new NewRow[]{createNewRow(customer, 1L, "xyz"),
                          createNewRow(customer, 2L, "abc"),
                          createNewRow(order, 11L, 1L, "ori"),
                          createNewRow(order, 12L, 1L, "david"),
                          createNewRow(order, 21L, 2L, "tom"),
                          createNewRow(order, 22L, 2L, "jack"),
                          createNewRow(item, 111L, 11L),
                          createNewRow(item, 112L, 11L),
                          createNewRow(item, 121L, 12L),
                          createNewRow(item, 122L, 12L),
                          createNewRow(item, 211L, 21L),
                          createNewRow(item, 212L, 21L),
                          createNewRow(item, 221L, 22L),
                          createNewRow(item, 222L, 22L)};
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void testCursorLifecycle(Operator scan, CursorLifecycleTestCase testCase, AkCollator ... collators)
    {
        Cursor cursor = cursor(scan, queryContext);
        // Check idle following creation
        assertTrue(cursor.isIdle());
        // Check active following open
        testCase.firstSetup();
        cursor.open();
        assertTrue(cursor.isActive());
        // Check idle following close
        cursor.close();
        assertTrue(cursor.isIdle());
        // Check active following re-open
        testCase.firstSetup();
        cursor.open();
        assertTrue(cursor.isActive());
        cursor.close();
        // Check active during iteration
        testCase.firstSetup();
        if (testCase.hKeyComparison()) {
            compareRenderedHKeys(testCase.firstExpectedHKeys(), cursor);
        } else {
            compareRows(testCase.firstExpectedRows(), cursor, collators);
        }
        assertTrue(cursor.isIdle());
        // Check close during iteration.
        if (testCase.hKeyComparison()
            ? testCase.firstExpectedHKeys().length > 1
            : testCase.firstExpectedRows().length > 1) {
            testCase.firstSetup();
            cursor.open();
            cursor.next();
            assertTrue(cursor.isActive());
            cursor.close();
            assertTrue(cursor.isIdle());
        }
        // Check that a second execution works
        testCase.secondSetup();
        if (testCase.hKeyComparison()) {
            compareRenderedHKeys(testCase.secondExpectedHKeys(), cursor);
        } else {
            compareRows(testCase.secondExpectedRows(), cursor, collators);
        }
        assertTrue(cursor.isIdle());
        // Check close of idle cursor is permitted
        try {
            cursor.close();
        } catch (CursorLifecycle.WrongStateException e) {
            fail();
        }
        // Check destroyed following destroy
        cursor.destroy();
        assertTrue(cursor.isDestroyed());
        // Check open after destroy disallowed
        try {
            testCase.firstSetup();
            cursor.open();
            fail();
        } catch (CursorLifecycle.WrongStateException e) {
            // expected
        }
        // Check next after destroy disallowed
        try {
            cursor.next();
            fail();
        } catch (CursorLifecycle.WrongStateException e) {
            // expected
        }
        // Check close after destroy disallowed
        try {
            cursor.close();
            fail();
        } catch (CursorLifecycle.WrongStateException e) {
            // expected
        }
    }

    protected void use(NewRow[] db)
    {
        writeRows(db);
    }

    protected Group group(int userTableId)
    {
        return getRowDef(userTableId).table().getGroup();
    }

    protected UserTable userTable(int userTableId)
    {
        RowDef userTableRowDef = getRowDef(userTableId);
        return userTableRowDef.userTable();
    }

    protected IndexRowType indexType(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<String>();
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

    protected TestRow row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    protected TestRow row(String hKeyString, RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields, hKeyString);
    }

    protected TestRow row(RowType rowType, Object[] fields, AkType[] types)
    {
        return new TestRow(rowType, new RowValuesHolder(fields, types), null);
    }

    protected RowBase row(int tableId, Object... values /* alternating field position and value */)
    {
        NewRow niceRow = createNewRow(tableId);
        int i = 0;
        while (i < values.length) {
            int position = (Integer) values[i++];
            Object value = values[i++];
            niceRow.put(position, value);
        }
        return PersistitGroupRow.newPersistitGroupRow(adapter, niceRow.toRowData());
    }

    protected RowBase row(IndexRowType indexRowType, Object... values) {
/*
        try {
*/
            ValuesHolderRow row = new ValuesHolderRow(indexRowType, usingPValues());
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (row.usingPValues()) {
                    PValue pvalue = row.pvalueAt(i);
                    if (value == null) {
                        pvalue.putNull();
                    } else if (value instanceof Integer) {
                        if (pvalue.getUnderlyingType() == PUnderlying.INT_64)
                            pvalue.putInt64(((Integer) value).longValue());
                        else
                            pvalue.putInt32((Integer) value);
                    } else if (value instanceof Long) {
                        if (pvalue.getUnderlyingType() == PUnderlying.INT_32)
                            pvalue.putInt32(((Long) value).intValue());
                        else
                            pvalue.putInt64((Long) value);
                    } else if (value instanceof String) {
                        pvalue.putString((String) value, null);
                    } else if (value instanceof BigDecimal) {
                        pvalue.putObject(new MBigDecimalWrapper((BigDecimal) value));
                    } else {
                        fail();
                    }
                }
                else {
                    ValueHolder valueHolder = row.holderAt(i);
                    if (value == null) {
                        valueHolder.putRawNull();
                    } else if (value instanceof Integer) {
                        valueHolder.putInt((Integer) value);
                    } else if (value instanceof Long) {
                        valueHolder.putLong((Long) value);
                    } else if (value instanceof String) {
                        valueHolder.putString((String) value);
                    } else if (value instanceof BigDecimal) {
                        valueHolder.putDecimal((BigDecimal) value);
                    } else {
                        fail();
                    }
                }
            }
            return row;
/*
            return new PersistitIndexRow(adapter, indexRowType, values);
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
        List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
        try {
            cursor.open();
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
    }

    @SuppressWarnings("unused") // useful for debugging
    protected void dumpToAssertion(Cursor cursor)
    {
        List<String> strings = new ArrayList<String>();
        try {
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                strings.add(String.valueOf(row));
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            cursor.close();
        }
        strings.add(0, strings.size() == 1 ? "1 string:" : strings.size() + " strings:");
        throw new AssertionError(Strings.join(strings));
    }

    @SuppressWarnings("unused") // useful for debugging
    protected void dumpToAssertion(Operator plan)
    {
        dumpToAssertion(cursor(plan, queryContext));
    }

    protected void dump(Cursor cursor)
    {
        cursor.open();
        Row row;
        while ((row = cursor.next()) != null) {
            System.out.println(String.valueOf(row));
        }
        cursor.close();
    }
    
    protected void dump(Operator plan)
    {
        dump(cursor(plan, queryContext));
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.open();
            count = 0;
            List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, count);
    }

    protected int ordinal(RowType rowType)
    {
        return rowType.userTable().rowDef().getOrdinal();
    }


    public Operator rowsToValueScan(Row... rows) {
        List<BindableRow> bindableRows = new ArrayList<BindableRow>();
        RowType type = null;
        for(Row row : rows) {
            RowType newType = row.rowType();
            if(type == null) {
                type = newType;
            } else if(type != newType) {
                fail("Multiple row types: " + type + " vs " + newType);
            }
            bindableRows.add(BindableRow.of(row, usingPValues()));
        }
        return API.valuesScan_Default(bindableRows, type);
    }


    protected int customer;
    protected int order;
    protected int item;
    protected int address;
    protected UserTableRowType customerRowType;
    protected UserTableRowType orderRowType;
    protected UserTableRowType itemRowType;
    protected UserTableRowType addressRowType;
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
    protected NewRow[] db;
    protected NewRow[] emptyDB = new NewRow[0];
    protected PersistitAdapter adapter;
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

        public RowBase[] firstExpectedRows()
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

        public RowBase[] secondExpectedRows()
        {
            return firstExpectedRows();
        }

        public String[] secondExpectedHKeys()
        {
            return firstExpectedHKeys();
        }
    }
}
