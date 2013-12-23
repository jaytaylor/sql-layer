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

package com.foundationdb.ais.util;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.types.service.TestTypesRegistry;

public final class DDLGeneratorTest {

    @Test
    public void testCreateTable() throws Exception {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "col", 0, "decimal unsigned", 11L, 3L, true, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("myGroup", "some_group_schema");
        builder.addTableToGroup("myGroup", "schema", "table");
        builder.groupingIsComplete();

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        DDLGenerator generator = new DDLGenerator();

        assertEquals("table",
                "create table `schema`.`table`(`col` decimal(11, 3) unsigned NULL) engine=akibandb",
                generator.createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnCharset() throws Exception {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "c1", 0, "varchar", 255L, null, true, false, "utf-16", null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` varchar(255) CHARACTER SET UTF16 COLLATE UCS_BINARY NULL) engine=akibandb",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnCollation() throws Exception {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "c1", 0, "varchar", 255L, null, true, false, null, "sv_se_ci");
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` varchar(255) COLLATE sv_se_ci NULL) engine=akibandb",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnNotNull() throws Exception {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "c1", 0, "int", null, null, false, false, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` int NOT NULL) engine=akibandb",
                    new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnAutoIncrement() throws Exception {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "c1", 0, "int", null, null, true, true, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` int NULL AUTO_INCREMENT) engine=akibandb",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testTimestampColumn() {
        AISBuilder builder = new AISBuilder(TestTypesRegistry.MCOMPAT);
        builder.table("schema", "table");
        builder.column("schema", "table", "c1", 0, "timestamp", null, null, true, false, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` timestamp NULL) engine=akibandb",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }
}
