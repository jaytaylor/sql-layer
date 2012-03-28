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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.util.GrowableByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public final class MessageTest {
    private static AkibanInformationSchema coiSchema(boolean withGroupIndex) throws Exception {
        SchemaFactory schemaFactory = new SchemaFactory("test");
        return schemaFactory.ais("create table test.c(id int not null primary key, name varchar(32));",
                                 "create table test.o(id int not null primary key, cid int, foo int, ",
                                 "grouping foreign key(cid) references c(id));",
                                 "create table test.i(id int not null primary key, oid int, ",
                                 "grouping foreign key(oid) references o(id));",
                                 withGroupIndex ? "create index foo on o(c.name, o.foo) using left join;" : "");
    }

    private static void serializeAndCompare(AkibanInformationSchema ais) throws Exception {
        GrowableByteBuffer buffer = new GrowableByteBuffer(1 << 19);
        new Writer(new MessageTarget(buffer)).save(ais);
        buffer.flip();
        AkibanInformationSchema newAis = new Reader(new MessageSource(buffer)).load();
        assertEquals("buffer left", 0, buffer.remaining());
        assertEquals("groups", ais.getGroups().keySet(), newAis.getGroups().keySet());
        assertEquals("user tables", ais.getUserTables().keySet(), newAis.getUserTables().keySet());
        assertEquals("group tables", ais.getGroupTables().keySet(), newAis.getGroupTables().keySet());
        assertEquals("joins", ais.getJoins().keySet(), newAis.getJoins().keySet());
        newAis.checkIntegrity();
    }

    @Test
    public void emptyAIS() throws Exception {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        serializeAndCompare(ais);
    }

    @Test
    public void coiAIS() throws Exception {
        AkibanInformationSchema ais = coiSchema(false);
        serializeAndCompare(ais);
    }

    @Test
    public void coiAISWithGroupIndex() throws Exception {
        final AkibanInformationSchema ais = coiSchema(true);
        final Table cTable = ais.getTable("test", "c");
        assertNotNull("c table not null", cTable);
        final Table oTable = ais.getTable("test", "o");
        assertNotNull("o table not null", oTable);
        final Group group = cTable.getGroup();
        assertSame("customer and order group", group, oTable.getGroup());
        ais.checkIntegrity();
        serializeAndCompare(ais);
    }

    @Test
    public void unicodeSchemaAndTableNames() throws Exception {
        NewAISBuilder builder = AISBBasedBuilder.create();
        AkibanInformationSchema ais =
            builder
            .userTable("test", "☃").colLong("id").pk("id")
            .userTable("☂", "rain").colLong("id").pk("id")
            .userTable("☾", "☽").colLong("id").pk("id")
            .ais();
        serializeAndCompare(ais);
    }
}
