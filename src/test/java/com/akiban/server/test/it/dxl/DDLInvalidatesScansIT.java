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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.TableDefinitionChangedException;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public final class DDLInvalidatesScansIT extends ITBase {
    private final static String SCHEMA = "mycoolschema";
    private static final String CUSTOMERS = "customers";
    private static final String ORDERS = "orders";
    private static final String SNOWMEN = "snowmen";

    @Test(expected=TableDefinitionChangedException.class)
    public void dropScannedTable() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, ORDERS, "date");
            ddl().dropTable(session(), tableName(SCHEMA, ORDERS));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test
    public void addGrandChildTable() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "PRIMARY");
            createItems();
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        ListRowOutput output = new ListRowOutput();
        dml().scanSome(session(), cursor, output);
        dml().closeCursor(session(), cursor);
        assertEquals("rows scanned", expectedCustomers(), output.getRows());
    }

    @Test
    public void addChildTable() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "PRIMARY");
            createAddresses();
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        ListRowOutput output = new ListRowOutput();
        dml().scanSome(session(), cursor, output);
        dml().closeCursor(session(), cursor);
        assertEquals("rows scanned", expectedCustomers(), output.getRows());
    }

    @Test
    public void dropDifferentTableInGroup() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "PRIMARY");
            ddl().dropTable(session(), tableName(SCHEMA, ORDERS));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        ListRowOutput output = new ListRowOutput();
        dml().scanSome(session(), cursor, output);
        dml().closeCursor(session(), cursor);
        assertEquals("rows scanned", expectedCustomers(), output.getRows());
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void dropTableScanOnGroup() throws InvalidOperationException {
        final CursorId cursor;
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            UserTable customerUtable = ais.getUserTable(SCHEMA, CUSTOMERS);
            GroupTable customerGtable = customerUtable.getGroup().getGroupTable();
            cursor = openFullScan(customerGtable.getTableId(), 1);
            ddl().dropTable(session(), tableName(SCHEMA, ORDERS));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void addTableScanOnGroup() throws InvalidOperationException {
        final CursorId cursor;
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            UserTable customerUtable = ais.getUserTable(SCHEMA, CUSTOMERS);
            GroupTable customerGtable = customerUtable.getGroup().getGroupTable();
            cursor = openFullScan(customerGtable.getTableId(), 1);
            createAddresses();
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void dropIndexScanOnGroup() throws InvalidOperationException {
        final CursorId cursor;
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            UserTable customerUtable = ais.getUserTable(SCHEMA, CUSTOMERS);
            GroupTable customerGtable = customerUtable.getGroup().getGroupTable();
            cursor = openFullScan(customerGtable.getTableId(), 1);
            ddl().dropTableIndexes(
                    session(),
                    customerUtable.getName(),
                    Collections.singleton(
                            customerUtable.getIndexes().iterator().next().getIndexName().getName()
                    ));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void addIndexScanOnGroup() throws InvalidOperationException {
        final CursorId cursor;
        try {
            AkibanInformationSchema ais = ddl().getAIS(session());
            UserTable customerUtable = ais.getUserTable(SCHEMA, CUSTOMERS);
            GroupTable customerGtable = customerUtable.getGroup().getGroupTable();
            cursor = openFullScan(customerGtable.getTableId(), 1);
            ddl().createIndexes(session(), Collections.singleton(createIndex()));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void dropScannedIndex() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, ORDERS, "date");
            ddl().dropTableIndexes(session(), tableName(SCHEMA, ORDERS), Collections.singleton("date"));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void dropScannedIndexButAnotherReplaces() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "name");
            indexId(SCHEMA, CUSTOMERS, "name");
            ddl().dropTableIndexes(session(), tableName(SCHEMA, CUSTOMERS), Collections.singleton("name"));
            indexId(SCHEMA, CUSTOMERS, "position");
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void dropDifferentIndex() throws InvalidOperationException {
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "name");
            ddl().dropTableIndexes(session(), tableName(SCHEMA, CUSTOMERS), Collections.singleton("position"));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    @Test(expected=TableDefinitionChangedException.class)
    public void addNewIndex() throws InvalidOperationException {
        
        final CursorId cursor;
        try {
            cursor = openFullScan(SCHEMA, CUSTOMERS, "name");
            ddl().createIndexes(session(), Collections.singleton(createIndex()));
        } catch (InvalidOperationException e) {
            throw new TestException(e);
        }

        scanExpectingException(cursor);
    }

    private void scanExpectingException(CursorId cursorId) throws InvalidOperationException {
        ListRowOutput output = new ListRowOutput();
        try {
            dml().scanSome(session(), cursorId, output);
            fail("Expected exception, but scanned: " + output.getRows().toString());
        } finally {
            dml().closeCursor(session(), cursorId);
        }
    }

    private Index createIndex() throws InvalidOperationException {
        UserTable customers = getUserTable(SCHEMA, CUSTOMERS);
        Index addIndex = new TableIndex(
                customers,
                "played_for_Bs",
                2,
                false,
                "KEY"
        );
        addIndex.addColumn(
                IndexColumn.create(
                        addIndex,
                        customers.getColumn("has_played_for_bruins"),
                        0,
                        true,
                        null
                )
        );
        return addIndex;
    }

    @Before
    public void setUpTables() throws InvalidOperationException{
        createTable(
                SCHEMA, CUSTOMERS,
                "id int not null primary key",
                "name varchar(32)",
                "position char(1)",
                "has_played_for_bruins char(1)"
        );
        createIndex(SCHEMA, CUSTOMERS, "name", "name");
        createIndex(SCHEMA, CUSTOMERS, "position", "position");
        int orders = createTable(
                SCHEMA, ORDERS,
                "id int not null primary key",
                "cid int",
                "date varchar(32)",
                "GROUPING FOREIGN KEY (cid) REFERENCES " + CUSTOMERS + " (id)"
        );
        createIndex(SCHEMA, ORDERS, "date", "date");
        int snowmen = createTable(
                SCHEMA, SNOWMEN,
                "id int not null primary key",
                "melt_at int"
        );

        List<NewRow> customerRows = expectedCustomers();
        writeRows(customerRows.toArray(new NewRow[customerRows.size()]));

        writeRows(
                createNewRow(orders, 31, 27, "today"),
                createNewRow(orders, 32, 27, "yesterday"),

                createNewRow(orders, 33, 29, "tomorrow"),
                createNewRow(orders, 34, 29, "never"),

                createNewRow(snowmen, 1, 32),
                createNewRow(snowmen, 2, -40) // C or F?!
        );
    }

    private List<NewRow> expectedCustomers() {
        int customers = tableId(SCHEMA, CUSTOMERS);
        return Arrays.asList(
                createNewRow(customers, 27L, "Knoepfli", "C", "N"), // I forget if he actually played center...
                createNewRow(customers, 29L, "Bitz", "W", "Y")

        );
    }

    private int createItems() throws InvalidOperationException {
        int tid = createTable(
                SCHEMA, "items",
                "id int not null primary key",
                "oid int",
                "sku int",
                "GROUPING FOREIGN KEY (oid) REFERENCES " + ORDERS + " (id)"
        );
        createIndex(SCHEMA, "items", "sku", "sku");
        return tid;
    }

    private int createAddresses() throws InvalidOperationException {
        return createTable(
                SCHEMA, "addresses",
                "id int not null primary key",
                "cid int",
                "street varchar(32)",
                "GROUPING FOREIGN KEY (cid) REFERENCES " + CUSTOMERS + " (id)"
        );
    }
}
