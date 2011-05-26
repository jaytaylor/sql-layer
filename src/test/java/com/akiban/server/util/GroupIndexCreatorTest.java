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

package com.akiban.server.util;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.server.util.GroupIndexCreator.GroupIndexCreatorException;
import static org.junit.Assert.assertEquals;

public class GroupIndexCreatorTest {
    private AkibanInformationSchema ais;

    @Before
    public void setup() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        ais = builder.defaultSchema("test")
                .userTable("c").colLong("id").colString("name", 32).pk("id")
                .userTable("o").colLong("id").colLong("date").colLong("cid").pk("id").joinTo("c").on("cid", "id")
                .ais();
    }

    @Test(expected=GroupIndexCreatorException.class)
    public void unknownGroup() throws Exception {
        GroupIndexCreator.createIndex(ais, "foobar", "name_date", "c.name");
    }

    @Test
    public void singleTableSingleColumn() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(ais, "c", "c_name", "c.name");
        final Group cGroup = ais.getGroup("c");
        final UserTable cTable = ais.getUserTable("test", "c");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("index name", "c_name", index.getIndexName().getName());
        assertEquals("column count", 1, index.getColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getColumns().get(0).getColumn());
    }

    @Test
    public void twoTablesTwoColumns() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(ais, "c", "name_date", "c.name,o.date");
        final Group cGroup = ais.getGroup("c");
        final UserTable cTable = ais.getUserTable("test", "c");
        final UserTable oTable = ais.getUserTable("test", "o");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("right name", "name_date", index.getIndexName().getName());
        assertEquals("column count", 2, index.getColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getColumns().get(0).getColumn());
        assertEquals("col2 is o.date", oTable.getColumn("date"), index.getColumns().get(1).getColumn());
    }
}
