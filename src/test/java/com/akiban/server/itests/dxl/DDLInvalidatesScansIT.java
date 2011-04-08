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

package com.akiban.server.itests.dxl;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.TableDefinitionChangedException;
import com.akiban.server.itests.ApiTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public final class DDLInvalidatesScansIT extends ApiTestBase {
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
            ddl().dropIndexes(
                    session(),
                    customerGtable.getName(),
                    Collections.singleton(
                        customerGtable.getIndexes().iterator().next().getIndexName().getName()
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
            ddl().dropIndexes(session(), tableName(SCHEMA, ORDERS), Collections.singleton("date"));
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
            int nameIndex = indexId(SCHEMA, CUSTOMERS, "name");
            ddl().dropIndexes(session(), tableName(SCHEMA, CUSTOMERS), Collections.singleton("name"));
            int positionIndex = indexId(SCHEMA, CUSTOMERS, "position");
            assertEquals("position index", nameIndex, positionIndex);
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
            ddl().dropIndexes(session(), tableName(SCHEMA, CUSTOMERS), Collections.singleton("position"));
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
        dml().scanSome(session(), cursorId, output);
        fail("Expected exception, but scanned: " + output.getRows().toString());
    }

    private Index createIndex() throws InvalidOperationException {
        UserTable customers = getUserTable(SCHEMA, CUSTOMERS);
        Index addIndex = new Index(
                customers,
                "played_for_Bs",
                2,
                false,
                "KEY"
        );
        addIndex.addColumn(
                new IndexColumn(
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
                "id int key",
                "name varchar(32)",
                "position char(1)",
                "has_played_for_bruins char(1)",
                "key (name)",
                "key (position)"
        );
        int orders = createTable(
                SCHEMA, ORDERS,
                "id int key",
                "cid int",
                "date varchar(32)",
                "key (date)",
                "CONSTRAINT __akiban_o FOREIGN KEY __akiban_o (cid) REFERENCES " + CUSTOMERS + " (id)"
        );
        int snowmen = createTable(
                SCHEMA, SNOWMEN,
                "id int key",
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
        return createTable(
                SCHEMA, "items",
                "id int key",
                "oid int",
                "sku int",
                "key (sku)",
                "CONSTRAINT __akiban_i FOREIGN KEY __akiban_i (oid) REFERENCES " + ORDERS + " (id)"
        );
    }

    private int createAddresses() throws InvalidOperationException {
        return createTable(
                SCHEMA, "addresses",
                "id int key",
                "cid int",
                "street varchar(32)",
                "CONSTRAINT __akiban_a FOREIGN KEY __akiban_a (cid) REFERENCES " + CUSTOMERS + " (id)"
        );
    }
}
