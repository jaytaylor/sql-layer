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

package com.akiban.ais.util;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibaInformationSchema;

public final class DDLGeneratorTest {

    @Test
    public void testCreateTable() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col", 0, "decimal unsigned", 11L, 3L, true, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("myGroup", "akiba_objects", "_group0");
        builder.addTableToGroup("myGroup", "schema", "table");
        builder.groupingIsComplete();

        AkibaInformationSchema ais = builder.akibaInformationSchema();
        DDLGenerator generator = new DDLGenerator();

        assertEquals("group table",
                "create table `akiba_objects`.`_group0`(`table$col` decimal(11, 3) unsigned, `table$__akiban_pk` bigint, key `table$PRIMARY`(`table$__akiban_pk`)) engine=akibandb",
                generator.createTable(ais.getGroup("myGroup").getGroupTable()));
    }
}
