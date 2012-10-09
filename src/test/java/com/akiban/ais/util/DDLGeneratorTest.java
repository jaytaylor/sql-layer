/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.ais.util;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;

public final class DDLGeneratorTest {

    @Test
    public void testCreateTable() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col", 0, "decimal unsigned", 11L, 3L, true, false, null, null);
        builder.basicSchemaIsComplete();
        builder.createGroup("myGroup", "some_group_schema", "_group0");
        builder.addTableToGroup("myGroup", "schema", "table");
        builder.groupingIsComplete();

        AkibanInformationSchema ais = builder.akibanInformationSchema();
        DDLGenerator generator = new DDLGenerator();

        assertEquals("table",
                "create table `schema`.`table`(`col` decimal(11, 3) unsigned) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
                generator.createTable(ais.getUserTable("schema", "table")));
    }

    @Test
    public void testColumnCharset() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "c1", 0, "varchar", 255L, null, true, false, "utf-16", null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` varchar(255) CHARACTER SET utf-16) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnCollation() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "c1", 0, "varchar", 255L, null, true, false, null, "euckr_korean_ci");
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` varchar(255) COLLATE euckr_korean_ci) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnNotNull() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "c1", 0, "int", null, null, false, false, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` int NOT NULL) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
                    new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }

    @Test
    public void testColumnAutoIncrement() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "c1", 0, "int", null, null, true, true, null, null);
        builder.basicSchemaIsComplete();
        AkibanInformationSchema ais = builder.akibanInformationSchema();
        assertEquals("create table `schema`.`table`(`c1` int AUTO_INCREMENT) engine=akibandb DEFAULT CHARSET=utf8 COLLATE=utf8_bin",
                     new DDLGenerator().createTable(ais.getTable("schema", "table")));
    }
}
