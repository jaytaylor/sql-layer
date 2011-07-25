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

import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.Arrays;
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

    
    @Test
    public void singleTableJustName() {
        createCTable();
        ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName(SCHEMA, "ahh"));
        expectTablesInSchema(SCHEMA, "ahh");
    }

    @Test
    public void singleTableNameAndSchema() {
        createCTable();
        ddl().renameTable(session(), tableName(SCHEMA, C_NAME), tableName("box", "cob"));
        expectTablesInSchema(SCHEMA);
        expectTablesInSchema("box", "cob");
    }

    @Test
    public void leafTableJustName() {
        createCTable();
        createATable();
        ddl().renameTable(session(), tableName(SCHEMA, A_NAME), tableName(SCHEMA, "dip"));
        expectTablesInSchema(SCHEMA, C_NAME, "dip");
    }

    @Test
    public void leafTableNameAndSchema() {
        createCTable();
        createATable();
        ddl().renameTable(session(), tableName(SCHEMA, A_NAME), tableName("eep", "fee"));
        expectTablesInSchema(SCHEMA, C_NAME);
        expectTablesInSchema("eep", "fee");
    }

    @Test
    public void middleTableJustName() {
        createCTable();
        createOTable();
        createITable();
        ddl().renameTable(session(), tableName(SCHEMA, I_NAME), tableName(SCHEMA, "goo"));
        expectTablesInSchema(SCHEMA, C_NAME, O_NAME, "goo");
    }

    @Test
    public void middleTableNameAndSchema() {
        createCTable();
        createOTable();
        createITable();
        ddl().renameTable(session(), tableName(SCHEMA, I_NAME), tableName("hat", "ice"));
        expectTablesInSchema(SCHEMA, C_NAME, O_NAME);
        expectTablesInSchema("hat", "ice");
    }
}
