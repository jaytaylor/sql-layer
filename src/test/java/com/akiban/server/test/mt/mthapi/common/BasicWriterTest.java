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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class BasicWriterTest {
    @Test
    public void simpleTableDDL() {
        SaisTable one = new SaisBuilder().table("zebra", "id", "stripes").pk("id").backToBuilder().getSoleRootTable();
        String ddl = DDLUtils.buildDDL(one, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE zebra(id int not null,stripes int, PRIMARY KEY (id))",
                ddl);
    }

    @Test
    public void multiColumnDDL() {
        SaisTable one = new SaisBuilder()
                .table("zebra", "id", "stripes", "height").pk("id", "stripes") // unique per zebra!
                .backToBuilder().getSoleRootTable();
        String ddl = DDLUtils.buildDDL(one, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE zebra(id int not null,stripes int not null,height int, PRIMARY KEY (id,stripes))",
                ddl);
    }

    @Test
    public void withJoin() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("top", "id1", "id2").pk("id1", "id2");
        builder.table("second", "tid1", "tid2").joinTo("top").col("id1", "tid1").col("id2", "tid2");
        SaisTable second = builder.getSoleRootTable().getChild("second");

        String ddl = DDLUtils.buildDDL(second, new StringBuilder());
        assertEquals("ddl",
                "CREATE TABLE second(tid1 int,tid2 int, "+
                "GROUPING FOREIGN KEY(tid1,tid2) REFERENCES top(id1,id2))",
                ddl);
    }
}
