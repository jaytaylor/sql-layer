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

package com.foundationdb.server.util;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.error.NoSuchGroupException;
import org.junit.Before;
import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GroupIndexCreatorTest {
    private static final String SCHEMA = "test";
    private static final TableName GROUP_C = new TableName(SCHEMA, "c");
    private AkibanInformationSchema ais;

    @Before
    public void setup() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        ais = builder.defaultSchema(SCHEMA)
                .table("c").colLong("id").colString("name", 32).pk("id")
                .table("o").colLong("id").colLong("date").colLong("cid").pk("id").joinTo("c").on("cid", "id")
                .unvalidatedAIS();
    }

    @Test(expected=NoSuchGroupException.class)
    public void unknownGroup() throws Exception {
        GroupIndexCreator.createIndex(null, ais, new TableName(SCHEMA, "foobar"), "name_date", "c.name", Index.JoinType.LEFT);
    }

    @Test
    public void singleTableSingleColumnLeft() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(null, ais, GROUP_C, "c_name", "c.name", Index.JoinType.LEFT);
        final Group cGroup = ais.getGroup(GROUP_C);
        final Table cTable = ais.getTable("test", "c");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("index name", "c_name", index.getIndexName().getName());
        assertEquals("column count", 1, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("join type", Index.JoinType.LEFT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }

    @Test
    public void singleTableSingleColumnRight() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(null, ais, GROUP_C, "c_name", "c.name", Index.JoinType.RIGHT);
        final Group cGroup = ais.getGroup(GROUP_C);
        final Table cTable = ais.getTable("test", "c");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("index name", "c_name", index.getIndexName().getName());
        assertEquals("column count", 1, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("join type", Index.JoinType.RIGHT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }

    @Test
    public void twoTablesTwoColumns() throws Exception {
        GroupIndex index = GroupIndexCreator.createIndex(null, ais, GROUP_C, "name_date", "c.name,o.date", Index.JoinType.LEFT);
        final Group cGroup = ais.getGroup(GROUP_C);
        final Table cTable = ais.getTable("test", "c");
        final Table oTable = ais.getTable("test", "o");
        assertEquals("group same", cGroup, index.getGroup());
        assertEquals("right name", "name_date", index.getIndexName().getName());
        assertEquals("column count", 2, index.getKeyColumns().size());
        assertEquals("col1 is c.name", cTable.getColumn("name"), index.getKeyColumns().get(0).getColumn());
        assertEquals("col2 is o.date", oTable.getColumn("date"), index.getKeyColumns().get(1).getColumn());
        assertEquals("join type", Index.JoinType.LEFT, index.getJoinType());
        assertTrue("join not valid", index.isValid());
    }
}
