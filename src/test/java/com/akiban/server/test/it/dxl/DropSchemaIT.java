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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class DropSchemaIT extends ITBase {
    private void expectTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final UserTable userTable = ais.getUserTable(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", userTable);
        }
    }

    private void expectNotTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final UserTable userTable = ais.getUserTable(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", userTable);
        }
    }


    @Test
    public void unknownSchemaIsNoOp() throws InvalidOperationException {
        createTable("test", "t", "id int key");
        ddl().dropSchema(session(), "not_a_real_schema");
        expectTables("test", "t");
    }

    @Test
    public void dropSchemaBasic() throws InvalidOperationException {
        createTable("test", "t", "id int key");
        ddl().dropSchema(session(), "test");
        expectNotTables("test", "t");
    }

    @Test
    public void dropSchemaSingleTableCheckData() throws InvalidOperationException {
        final int tid1 = createTable("test", "t", "id int key");
        writeRows(createNewRow(tid1, 1L), createNewRow(tid1, 2L));

        ddl().dropSchema(session(), "test");
        expectNotTables("test", "t");

        // Check for lingering data
        final int tid2 = createTable("test", "t", "id int key");
        expectRowCount(tid2, 0);
        assertEquals("scanned rows", 0, scanFull(scanAllRequest(tid2)).size());
    }

    @Test
    public void dropSchemaMultipleTables() throws InvalidOperationException {
        createTable("s1", "a", "id int key");
        createTable("s1", "b", "id int key");
        createTable("s2", "a", "id int key");
        createTable("s2", "b", "id int key");
        ddl().dropSchema(session(), "s1");
        expectNotTables("s1", "a", "b");
        expectTables("s2", "a", "b");
    }

    @Test
    public void dropSchemaGroupedTables() throws InvalidOperationException {
        createTable("s1", "c", "id int key");
        createTable("s1", "o", "id int key, cid int, constraint __akiban foreign key(cid) references c(id)");
        createTable("s1", "i", "id int key, oid int, constraint __akiban foreign key(oid) references o(id)");
        createTable("s1", "t", "id int key");
        createTable("s2", "c", "id int key");
        ddl().dropSchema(session(), "s1");
        expectNotTables("s1", "c", "o", "i", "t");
        expectTables("s2", "c");
    }

    @Test
    public void dropSchemaCrossSchemaGroup() throws InvalidOperationException {
        createTable("s1", "c", "id int key");
        createTable("s1", "o", "id int key, cid int, constraint __akiban foreign key(cid) references c(id)");
        createTable("s2", "i", "id int key, oid int, constraint __akiban foreign key(oid) references s1.o(id)");
        try {
            ddl().dropSchema(session(), "s1");
            Assert.fail("InvalidOperationException expected");
        } catch(InvalidOperationException e) {
            // expected
        }
        expectNotTables("s1", "c", "o");
        expectNotTables("s2", "i");
    }
}
