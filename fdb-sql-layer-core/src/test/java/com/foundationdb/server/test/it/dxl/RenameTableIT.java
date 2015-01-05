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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.error.DuplicateTableNameException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class RenameTableIT extends ITBase {
    private static final String SCHEMA = "test";
    private static final String C_NAME = "c";
    private static final String A_NAME = "a";
    private static final String O_NAME = "o";
    private static final String I_NAME = "i";

    private void createCTable() {
        createTable(SCHEMA, C_NAME, "id int not null, primary key(id)");
    }

    private void createATable() {
        createTable(SCHEMA, A_NAME, "id int not null, cid int, primary key(id)", akibanFK("cid", C_NAME, "id"));
    }

    private void createOTable() {
        createTable(SCHEMA, O_NAME, "id int not null, cid int, primary key(id)", akibanFK("cid", C_NAME, "id"));
    }

    private void createITable() {
        createTable(SCHEMA, I_NAME, "id int not null, oid int, primary key(id)", akibanFK("oid", O_NAME, "id"));
    }

    private int writeCRows() {
        int cid = tableId(SCHEMA, C_NAME);
        return writeRows(row(cid, 1L),
                         row(cid, 2L),  // no As, Os
                         row(cid, 3L),  // no Os
                         row(cid, 5L)).size();
    }

    private int writeARows() {
        int aid = tableId(SCHEMA, A_NAME);
        return writeRows(row(aid, 1L, 1L),
                         row(aid, 2L, 3L),
                         row(aid, 3L, 4L), // orphan
                         row(aid, 5L, 5L)).size();
    }

    private int writeORows() {
        int oid = tableId(SCHEMA, O_NAME);
        return writeRows(row(oid, 1L, 1L),
                         row(oid, 2L, 1L), // no Is
                         row(oid, 3L, 2L),
                         row(oid, 4L, 2L),
                         row(oid, 5L, 4L), // orphan
                         row(oid, 6L, 4L), // orphan
                         row(oid, 9L, 5L)).size();
    }

    private int writeIRows() {
        int iid = tableId(SCHEMA, I_NAME);
        return writeRows(row(iid, 1L, 1L),
                         row(iid, 2L, 1L),
                         row(iid, 3L, 3L),
                         row(iid, 4L, 3L),
                         row(iid, 5L, 4L),
                         row(iid, 6L, 5L),
                         row(iid, 7L, 6L),
                         row(iid, 8L, 6L),
                         row(iid, 9L, 7L), // orphan
                         row(iid, 10L, 7L), // orphan
                         row(iid, 11L, 9L)).size();

    }
    
    private void expectTablesInSchema(String schemaName, String... tableNames) {
        Set<String> actualInSchema = new TreeSet<>();
        for(Table table : ddl().getAIS(session()).getTables().values()) {
            if(table.getName().getSchemaName().equals(schemaName)) {
                actualInSchema.add(table.getName().getTableName());
            }
        }
        Set<String> expectedInSchema = new TreeSet<>();
        expectedInSchema.addAll(Arrays.asList(tableNames));

        assertEquals("Tables in schema " + schemaName,
                     expectedInSchema, actualInSchema);
    }

    private void expectStatusAndScanCount(String schemaName, String tableName, long rowCount) {
        int id = tableId(schemaName, tableName);
        expectRowCount(id, rowCount);
        List<Row> rows = scanAll(id);
        assertEquals("Scan rows: " + rows, rowCount, rows.size());
    }


    @Test(expected= NoSuchTableException.class)
    public void nonExistingTable() {
        ddl().renameTable(session(), tableName("a", "b"), tableName("b", "c"));
    }

    @Test
    public void duplicateInSameSchema() {
        final String NEW_NAME = "new_name";
        createCTable();
        int rowCount = writeCRows();
        createTable(SCHEMA, NEW_NAME, "id int not null primary key");
        try {
            ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(SCHEMA, NEW_NAME));
            Assert.fail("Expected DuplicateTableNameException");
        }
        catch(DuplicateTableNameException e) {
        }
        expectTablesInSchema(SCHEMA, C_NAME, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, rowCount);
        expectStatusAndScanCount(SCHEMA, NEW_NAME, 0);
    }

    @Test
    public void duplicateInDifferentSchema() {
        final String NEW_SCHEMA = "new_schema";
        final String NEW_NAME = "new_name";
        createCTable();
        int rowCount = writeCRows();
        createTable(NEW_SCHEMA, NEW_NAME, "id int not null primary key");
        try {
            ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(NEW_SCHEMA, NEW_NAME));
            Assert.fail("Expected DuplicateTableNameException");
        }
        catch(DuplicateTableNameException e) {
        }
        expectTablesInSchema(SCHEMA, C_NAME);
        expectTablesInSchema(NEW_SCHEMA, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, rowCount);
        expectStatusAndScanCount(NEW_SCHEMA, NEW_NAME, 0);
    }

    @Test
    public void toSystemSchema() {
        createCTable();
        int rowCount = writeCRows();
        try {
            ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(TableName.INFORMATION_SCHEMA, C_NAME));
            Assert.fail("Expected ProtectedTableDDLException");
        }
        catch(ProtectedTableDDLException e) {
        }
        expectTablesInSchema(SCHEMA, C_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, rowCount);
    }

    @Test
    public void singleTableJustName() {
        final String NEW_NAME = "ahh";
        createCTable();
        int rowCount = writeCRows();
        ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, NEW_NAME, rowCount);
    }

    @Test
    public void singleTableNameAndSchema() {
        final String NEW_SCHEMA = "box";
        final String NEW_NAME = "cob";
        createCTable();
        int rowCount = writeCRows();
        ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(NEW_SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA);
        expectTablesInSchema(NEW_SCHEMA, NEW_NAME);
        expectStatusAndScanCount(NEW_SCHEMA, NEW_NAME, rowCount);
    }

    @Test
    public void leafTableJustName() {
        final String NEW_NAME = "DIP";
        createCTable();
        createATable();
        int cCount = writeCRows();
        int aCount = writeARows();
        ddl().renameTable(session(), tableName(SCHEMA, A_NAME), tableName(SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA, C_NAME, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, cCount);
        expectStatusAndScanCount(SCHEMA, NEW_NAME, aCount);
    }

    @Test
    public void leafTableNameAndSchema() {
        final String NEW_SCHEMA = "eep";
        final String NEW_NAME = "fee";
        createCTable();
        createATable();
        int cCount = writeCRows();
        int aCount = writeARows();
        ddl().renameTable(session(), tableName(SCHEMA, A_NAME), tableName(NEW_SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA, C_NAME);
        expectTablesInSchema(NEW_SCHEMA, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, cCount);
        expectStatusAndScanCount(NEW_SCHEMA, NEW_NAME, aCount);
    }

    @Test
    public void middleTableJustName() {
        final String NEW_NAME = "goo";
        createCTable();
        createOTable();
        createITable();
        int cCount = writeCRows();
        int oCount = writeORows();
        int iCount = writeIRows();
        ddl().renameTable(session(), tableName(SCHEMA, O_NAME), tableName(SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA, C_NAME, NEW_NAME, I_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, cCount);
        expectStatusAndScanCount(SCHEMA, NEW_NAME, oCount);
        expectStatusAndScanCount(SCHEMA, I_NAME, iCount);
    }

    @Test
    public void middleTableNameAndSchema() {
        final String NEW_SCHEMA = "hat";
        final String NEW_NAME = "ice";
        createCTable();
        createOTable();
        createITable();
        int cCount = writeCRows();
        int oCount = writeORows();
        int iCount = writeIRows();
        ddl().renameTable(session(), tableName(SCHEMA, O_NAME), tableName(NEW_SCHEMA, NEW_NAME));
        expectTablesInSchema(SCHEMA, C_NAME, I_NAME);
        expectTablesInSchema(NEW_SCHEMA, NEW_NAME);
        expectStatusAndScanCount(SCHEMA, C_NAME, cCount);
        expectStatusAndScanCount(NEW_SCHEMA, NEW_NAME, oCount);
        expectStatusAndScanCount(SCHEMA, I_NAME, iCount);
    }

    @Test
    public void renameAllSchemasAndTablesParentDownWithRestartsBetween() throws Exception {
        final TableName NEW_NAMES[] = { tableName("j","k"), tableName("l","m"),
                                        tableName("n","o"), tableName("p","q") };
        TableName curNames[] = { tableName(SCHEMA, C_NAME), tableName(SCHEMA, A_NAME),
                                 tableName(SCHEMA, O_NAME), tableName(SCHEMA, I_NAME) };
        
        createCTable();
        createATable();
        createOTable();
        createITable();
        final int COUNTS[] = { writeCRows(), writeARows(), writeORows(), writeIRows() };

        for(int i = 0; i < NEW_NAMES.length; ++i) {
            ddl().renameTable(session(), curNames[i], NEW_NAMES[i]);
            curNames[i] = NEW_NAMES[i];

            safeRestartTestServices();

            for(int j = 0; j < curNames.length; ++j) {
                TableName tn = curNames[j];
                expectStatusAndScanCount(tn.getSchemaName(), tn.getTableName(), COUNTS[j]);
            }

        }
    }

    /**
     * bug999467, simulate many repeated alters
     */
    @Test
    public void renameSameTablesMultipleTimes() {
        final int LOOPS = 3;
        final String COL_DEFS = "c1 INT";
        final TableName NAME1 = tableName("test", "t1");
        final TableName NAME2 = tableName("test", "sql#foo-1");
        final TableName NAME3 = tableName("test", "sql#foo_1");

        int initialTid = createTable(NAME1, COL_DEFS);
        writeRows(
                row(initialTid, 1),
                row(initialTid, 2),
                row(initialTid, 3)
        );

        for(int i = 1; i <= LOOPS; ++i) {
            // Create new table, copy old table from a pk scan
            int tid2 = createTable(NAME2, COL_DEFS);
            List<Row> pkRows = scanAllIndex(getTable(tableId(NAME1)).getPrimaryKeyIncludingInternal().getIndex());
            assertEquals("Row scanned from original on loop "+i, 3, pkRows.size());
            for(Row row : pkRows) {
                writeRow(tid2, row.value(0).getInt64());
            }
            // Rename both
            ddl().renameTable(session(), NAME1, NAME3);
            ddl().renameTable(session(), NAME2, NAME1);
            ddl().dropTable(session(), NAME3);
            // Confirm
            List<Row> newTableRows = scanAll(tableId(NAME1));
            assertEquals("Rows scanned after renames and drop on loop "+i, 3, newTableRows.size());
        }
    }
}
