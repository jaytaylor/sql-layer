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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ProtectedTableDDLException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.test.it.ITBase;
import junit.framework.Assert;
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
        createTable(SCHEMA, C_NAME, "id int, primary key(id)");
    }

    private void createATable() {
        createTable(SCHEMA, A_NAME, "id int, cid int, primary key(id)", akibanFK("cid", C_NAME, "id"));
    }

    private void createOTable() {
        createTable(SCHEMA, O_NAME, "id int, cid int, primary key(id)", akibanFK("cid", C_NAME, "id"));
    }

    private void createITable() {
        createTable(SCHEMA, I_NAME, "id int, oid int, primary key(id)", akibanFK("oid", O_NAME, "id"));
    }

    private int writeCRows() {
        int cid = tableId(SCHEMA, C_NAME);
        return writeRows(createNewRow(cid, 1L),
                         createNewRow(cid, 2L),  // no As, Os
                         createNewRow(cid, 3L),  // no Os
                         createNewRow(cid, 5L));
    }

    private int writeARows() {
        int aid = tableId(SCHEMA, A_NAME);
        return writeRows(createNewRow(aid, 1L, 1L),
                         createNewRow(aid, 2L, 3L),
                         createNewRow(aid, 3L, 4L), // orphan
                         createNewRow(aid, 5L, 5L));
    }

    private int writeORows() {
        int oid = tableId(SCHEMA, O_NAME);
        return writeRows(createNewRow(oid, 1L, 1L),
                         createNewRow(oid, 2L, 1L), // no Is
                         createNewRow(oid, 3L, 2L),
                         createNewRow(oid, 4L, 2L),
                         createNewRow(oid, 5L, 4L), // orphan
                         createNewRow(oid, 6L, 4L), // orphan
                         createNewRow(oid, 9L, 5L));
    }

    private int writeIRows() {
        int iid = tableId(SCHEMA, I_NAME);
        return writeRows(createNewRow(iid, 1L, 1L),
                         createNewRow(iid, 2L, 1L),
                         createNewRow(iid, 3L, 3L),
                         createNewRow(iid, 4L, 3L),
                         createNewRow(iid, 5L, 4L),
                         createNewRow(iid, 6L, 5L),
                         createNewRow(iid, 7L, 6L),
                         createNewRow(iid, 8L, 6L),
                         createNewRow(iid, 9L, 7L), // orphan
                         createNewRow(iid, 10L, 7L), // orphan
                         createNewRow(iid, 11L, 9L));

    }
    
    private void expectTablesInSchema(String schemaName, String... tableNames) {
        Set<String> actualInSchema = new TreeSet<String>();
        for(UserTable table : ddl().getAIS(session()).getUserTables().values()) {
            if(table.getName().getSchemaName().equals(schemaName)) {
                actualInSchema.add(table.getName().getTableName());
            }
        }
        Set<String> expectedInSchema = new TreeSet<String>();
        expectedInSchema.addAll(Arrays.asList(tableNames));

        assertEquals("Tables in schema " + schemaName,
                     expectedInSchema, actualInSchema);
    }

    private void expectStatusAndScanCount(String schemaName, String tableName, long rowCount) {
        updateAISGeneration();
        int id = tableId(schemaName, tableName);
        expectRowCount(id, rowCount);
        List<RowData> rows = scanFull(scanAllRequest(id));
        if(rowCount != rows.size()) {
            assertEquals("Scan rows: " + rows, rowCount, rows.size());
        }
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
        createTable(SCHEMA, NEW_NAME, "id int key");
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
        createTable(NEW_SCHEMA, NEW_NAME, "id int key");
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
            ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(TableName.AKIBAN_INFORMATION_SCHEMA, C_NAME));
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
}
