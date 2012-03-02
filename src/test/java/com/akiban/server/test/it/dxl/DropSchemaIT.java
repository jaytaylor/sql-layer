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
import com.akiban.server.error.ForeignConstraintDDLException;
import com.akiban.server.error.InvalidOperationException;
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
        createTable("one", "t", "id int not null primary key");
        ddl().dropSchema(session(), "not_a_real_schema");
        expectTables("one", "t");
    }

    @Test
    public void singleTable() throws InvalidOperationException {
        createTable("one", "t", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t");
    }

    @Test
    public void singleTableCheckData() throws InvalidOperationException {
        final int tid1 = createTable("one", "t", "id int not null primary key");
        writeRows(createNewRow(tid1, 1L), createNewRow(tid1, 2L));
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t");
        // Check for lingering data
        final int tid2 = createTable("one", "t", "id int not null primary key");
        expectRowCount(tid2, 0);
        assertEquals("scanned rows", 0, scanFull(scanAllRequest(tid2)).size());
    }

    @Test
    public void multipleTables() throws InvalidOperationException {
        createTable("one", "a", "id int not null primary key");
        createTable("one", "b", "id int not null primary key");
        createTable("two", "a", "id int not null primary key");
        createTable("two", "b", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "a", "b");
        expectTables("two", "a", "b");
    }

    @Test
    public void groupedTables() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("one", "i", "id int not null primary key, oid int, grouping foreign key(oid) references o(id)");
        createTable("one", "t", "id int not null primary key");
        createTable("two", "c", "id int not null primary key");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "c", "o", "i", "t");
        expectTables("two", "c");
    }

    @Test
    public void crossSchemaGroupInvalid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("two", "i", "id int not null primary key, oid int, grouping foreign key(oid) references one.o(id)");
        try {
            ddl().dropSchema(session(), "one");
            Assert.fail("ForeignConstraintDDLException expected");
        } catch(ForeignConstraintDDLException e) {
            // expected
        }
        expectTables("one", "c", "o");
        expectTables("two", "i");
    }

    @Test
    public void crossSchemaGroupValid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("one", "o", "id int not null primary key, cid int, grouping foreign key(cid) references c(id)");
        createTable("two", "i", "id int not null primary key, oid int, grouping foreign key(oid) references one.o(id)");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c", "o");
        expectNotTables("two", "i");
    }
}
