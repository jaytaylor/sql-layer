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

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.server.error.ForeignKeyPreventsDropTableException;
import com.foundationdb.server.error.ForeignConstraintDDLException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchSchemaException;
import com.foundationdb.server.error.ViewReferencesExist;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class DropSchemaIT extends ITBase {
    private void expectTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final Table table = ais.getTable(schemaName, name);
            assertNotNull(schemaName + " " + name + " doesn't exist", table);
        }
    }

    private void expectNotTables(String schemaName, String... tableNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : tableNames) {
            final Table table = ais.getTable(schemaName, name);
            assertNull(schemaName + " " + name + " still exists", table);
        }
    }

    private void expectSequences (String schemaName, String... sequenceNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sequenceNames) {
            final Sequence sequence = ais.getSequence(new TableName(schemaName, name));
            assertNotNull (schemaName + "." + name + " doesn't exist", sequence);
        }
        
    }
    private void expectNotSequence (String schemaName, String... sequenceNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : sequenceNames) {
            final Sequence sequence = ais.getSequence(new TableName(schemaName, name));
            assertNull (schemaName + "." + name + " still exists", sequence);
        }
    }

    private void expectViews(String schemaName, String... viewNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : viewNames) {
            final View view = ais.getView(new TableName(schemaName, name));
            assertNotNull (schemaName + "." + name + " doesn't exist", view);
        }
        
    }
    private void expectNotViews(String schemaName, String... viewNames) {
        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (String name : viewNames) {
            final View view = ais.getView(new TableName(schemaName, name));
            assertNull (schemaName + "." + name + " still exists", view);
        }
    }

    @After
    public void lookForDanglingTrees() throws Exception {
        super.lookForDanglingTrees();
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

    @Test
    public void crossSchemaGroupValid2() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("two", "o", "id int not null primary key, cid int, grouping foreign key(cid) references one.c(id)");
        createTable("two", "i", "id int not null primary key, oid int, grouping foreign key(oid) references o(id)");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c");
        expectNotTables("two", "o", "i");
    }

    @Test
    public void crossSchemaForeignKeyInvalid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("two", "o", "id int not null primary key, cid int, foreign key(cid) references one.c(id)");
        try {
            ddl().dropSchema(session(), "one");
            Assert.fail("ForeignConstraintDDLException expected");
        } catch(ForeignKeyPreventsDropTableException e) {
            // expected
        }
        expectTables("one", "c");
        expectTables("two", "o");
    }

    @Test
    public void crossSchemaForeignKeyValid() throws InvalidOperationException {
        createTable("one", "c", "id int not null primary key");
        createTable("two", "o", "id int not null primary key, cid int, foreign key(cid) references one.c(id)");
        ddl().dropSchema(session(), "two");
        expectTables("one", "c");
        expectNotTables("two", "o");
    }

    @Test
    public void dropSchemaSequence() throws InvalidOperationException {
        createTable("one", "o", "id int not null PRIMARY KEY generated by default as identity (start with 1)");
        createSequence("one", "seq1", "Start with 1 increment by 1 no cycle");
        ddl().dropSchema(session(), "one");
        expectNotSequence("one", "seq1");
        expectNotTables("one", "o");
    }

    @Test
    public void dropSchemaOtherSequence() throws InvalidOperationException {
        AISBuilder builder = new AISBuilder();
        Table table = builder.table("keep", "c");
        Column column = builder.column("keep", "c", "id", 0, MNumeric.INT.instance(false), false, null, null);
        Sequence sequence = builder.sequence("drop", "thesequence", 1, 10, 1, 1000, false);
        column.setIdentityGenerator(sequence);
        ddl().createSequence(session(), sequence);
        ddl().createTable(session(), table);
        expectSequences("drop", "thesequence");
        createTable("drop", "o", "id int not null PRIMARY KEY generated by default as identity (start with 1)");
        try {
            ddl().dropSchema(session(), "drop");
            Assert.fail("ForeignConstraintDDLException expected");
        } catch(ForeignConstraintDDLException e) {
            // expected
        }
        expectTables("keep", "c");
        expectNotTables("drop", "o");
        expectSequences("drop", "thesequence");
    }

    @Test
    public void dropSchemaOtherSequenceValid() throws InvalidOperationException {
        AISBuilder builder = new AISBuilder();
        Table table = builder.table("drop", "c");
        Column column = builder.column("drop", "c", "id", 0, MNumeric.INT.instance(false), false, null, null);
        Sequence sequence = builder.sequence("keep", "thesequence", 1, 10, 1, 1000, false);
        column.setIdentityGenerator(sequence);
        ddl().createSequence(session(), sequence);
        ddl().createTable(session(), table);
        expectSequences("keep", "thesequence");
        createTable("keep", "o", "id int not null PRIMARY KEY generated by default as identity (start with 1)");
        ddl().dropSchema(session(), "drop");
        expectTables("keep", "o");
        expectNotTables("drop", "c");
        expectNotSequence("keep", "thesequence");
    }

    @Test
    public void dropViewValidInSchema() throws Exception {
        createTable("one", "t1",
                    "id int not null primary key", "name varchar(128)");
        createTable("two", "t2",
                    "id int not null primary key", "name varchar(128)");
        createView("one", "v1",
                   "SELECT * FROM t1");
        createView("two", "v2",
                   "SELECT * FROM t2");
        ddl().dropSchema(session(), "one");
        expectNotTables("one", "t1");
        expectNotViews("one", "v1");
        expectTables("two", "t2");
        expectViews("two", "v2");
    }

    @Test
    public void dropViewInvalidOutsideSchema() throws Exception {
        createTable("one", "t1",
                    "id int not null primary key", "name varchar(128)");
        createTable("two", "t2",
                    "id int not null primary key", "name varchar(128)");
        createView("one", "crossview",
                   "SELECT t1.id,t1.name,t2.name AS name2 FROM one.t1 t1, two.t2 t2 WHERE t1.id = t2.id");
        try {
            ddl().dropSchema(session(), "one");
        } catch (ViewReferencesExist ex) {
            // expected
        }
        expectTables("one", "t1");
        expectTables("two", "t2");
        expectViews("one", "crossview");
    }

    @Test
    public void dropViewsInOrder() throws Exception {
        createTable("test", "t1",
                    "id int not null primary key", "name varchar(128)");
        createView("test", "v1",
                   "SELECT * FROM t1");
        createView("test", "v3",
                   "SELECT * FROM v1");
        createView("test", "v2",
                   "SELECT * FROM v3");
        ddl().dropSchema(session(), "test");
        expectNotViews("test", "v1", "v2", "v3");
    }

    @Test
    public void createDropRecreateDropAndRestart() throws Exception {
        createTable("test2", "customer", "id int not null primary key");
        createTable("test2", "order", "name varchar(32)");
        createTable("test2", "item", "cost int");

        ddl().dropSchema(session(), "test2");
        expectNotTables("test2", "customer", "order", "item");

        createTable("test2", "order", "id int not null primary key");
        createTable("test2", "customer", "id int not null primary key, oid int, grouping foreign key(oid) references \"order\"(id)");

        ddl().dropSchema(session(), "test2");
        expectNotTables("test2", "customer", "order");

        safeRestartTestServices();
        expectNotTables("test2", "customer", "order");
    }
}
