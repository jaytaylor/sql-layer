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

package com.akiban.ais.metamodel.io;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.util.GrowableByteBuffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TableSubsetWriterTest {
    private AkibanInformationSchema srcAIS;

    @Before
    public void setUp() {
        AISBuilder builder = new AISBuilder();
        builder.setTableIdOffset(1);
        builder.userTable("foo", "bar");
        builder.column("foo", "bar", "id", 0, "int", null, null, false, false, null, null);
        builder.userTable("t", "c");
        builder.column("t", "c", "id", 0, "int", null, null, false, false, null, null);
        builder.index("t", "c", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("t", "c", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        builder.userTable("t", "o");
        builder.column("t", "o", "id", 0, "int", null, null, false, false, null, null);
        builder.column("t", "o", "cid", 1, "int", null, null, false, false, null, null);
        builder.joinTables("co", "t", "c", "t", "o");
        builder.joinColumns("co", "t", "c", "id", "t", "o", "cid");
        builder.basicSchemaIsComplete();
        builder.createGroup("c", "t", "__akiban_c");
        builder.addJoinToGroup("c", "co", 0);
        builder.createGroup("bar", "foo", "__akiban_bar");
        builder.addTableToGroup("bar", "foo", "bar");
        srcAIS = builder.akibanInformationSchema();
        srcAIS.checkIntegrity();
    }

    @After
    public void tearDown() {
        srcAIS = null;
    }


    @Test
    public void allTables() throws Exception {
        GrowableByteBuffer buffer1 = new GrowableByteBuffer(4096);
        GrowableByteBuffer buffer2 = new GrowableByteBuffer(4096);

        new Writer(new MessageTarget(buffer1)).save(srcAIS);

        new TableSubsetWriter(new MessageTarget(buffer2)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return true;
            }
        }.save(srcAIS);

        // Basic smoke test (buffer sizes), but can't compare actual
        // content as Writer interface imposes no ordering. More checks
        // are done in the following tests.
        buffer1.flip();
        buffer2.flip();
        assertEquals("limit", buffer1.limit(), buffer2.limit());
    }
    
    @Test
    public void tSchemaOnlyCheckStaticData() throws Exception {
        AkibanInformationSchema dstAIS = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(dstAIS)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().getSchemaName().equals("t");
            }
        }.save(srcAIS);

        assertEquals("type count", srcAIS.getTypes().size(), dstAIS.getTypes().size());
    }

    @Test
    public void tSchemaOnlyCheckAll() throws Exception {
        AkibanInformationSchema dstAIS = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(dstAIS)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().getSchemaName().equals("t");
            }
        }.save(srcAIS);

        dstAIS.checkIntegrity();
        assertNotNull("t.c exists", dstAIS.getUserTable("t", "c"));
        assertNotNull("t.o exists", dstAIS.getUserTable("t", "o"));
        assertEquals("user table count", 2, dstAIS.getUserTables().size());
        assertNotNull("co join exists", dstAIS.getJoin("co"));
        assertEquals("join count", 1, dstAIS.getJoins().size());
        assertNotNull("t.__akiban_c exists", dstAIS.getGroupTable("t", "__akiban_c"));
        assertEquals("group table count", 1, dstAIS.getGroupTables().size());
        assertNotNull("c group exists", dstAIS.getGroup("c"));
        assertEquals("group count", 1, dstAIS.getGroups().size());
    }

    @Test
    public void fooSchemaOnlyCheckAll() throws Exception {
        AkibanInformationSchema dstAIS = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(dstAIS)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().getSchemaName().equals("foo");
            }
        }.save(srcAIS);

        dstAIS.checkIntegrity();
        assertNotNull("foo.bar exists", dstAIS.getUserTable("foo", "bar"));
        assertEquals("user table count", 1, dstAIS.getUserTables().size());
        assertEquals("join count", 0, dstAIS.getJoins().size());
        assertNotNull("foo.__akiban_bar exists", dstAIS.getGroupTable("foo", "__akiban_bar"));
        assertEquals("group table count", 1, dstAIS.getGroupTables().size());
        assertNotNull("bar group exists", dstAIS.getGroup("bar"));
        assertEquals("group count", 1, dstAIS.getGroups().size());
    }

    @Test
    public void singleTableBreaksGroup() throws Exception {
        AkibanInformationSchema dstAIS = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(dstAIS)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getName().getTableName().equals("c");
            }
        }.save(srcAIS);

        // try/catch below as above is expected to succeed
        
        try {
            dstAIS.checkIntegrity(); // Group is missing its GroupTable
            Assert.fail("Exception expected!");
        }
        catch(IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void singleGroupCheckColumns() throws Exception {
        AkibanInformationSchema dstAIS = new AkibanInformationSchema();
        new TableSubsetWriter(new AISTarget(dstAIS)) {
            @Override
            public boolean shouldSaveTable(Table table) {
                return table.getGroup().getName().equals("c");
            }
        }.save(srcAIS);

        dstAIS.checkIntegrity();
        Table cTable = dstAIS.getUserTable("t", "c");
        assertNotNull("t.c exists", cTable);
        for(Column c : cTable.getColumns()) {
            assertNotNull(c.getName() + " has group column", c.getGroupColumn());
        }
        Table oTable = dstAIS.getUserTable("t", "o");
        assertNotNull("t.o exists", oTable);
        for(Column c : oTable.getColumns()) {
            assertNotNull(c.getName() + " has group column", c.getGroupColumn());
        }
        Table groupTable = dstAIS.getGroupTable("t", "__akiban_c");
        for(Column c : groupTable.getColumns()) {
            assertNotNull(c.getName() + " has user column", c.getUserColumn());
        }
        assertNotNull("t.__akiban_c exists", groupTable);
    }
}
