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

package com.akiban.ais.ddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.io.PrintWriter;
import java.io.StringWriter;
import com.akiban.ais.model.AkibanInformationSchema;
import org.junit.Test;
import com.akiban.ais.io.CSVTarget;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.UserTable;

public class DDLSourceTest {
    private final static String XXXXXXXX_DDL_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";
    private final static String XXXXXXXX_FK_DDL_FILE_NAME = "src/test/resources/xxxxxxxx_schema.ddl";

    @Test
    public void testFKParse() throws Exception {
        final AkibanInformationSchema ais1 = new DDLSource()
                .buildAISFromFile(XXXXXXXX_DDL_FILE_NAME);
        final AkibanInformationSchema ais2 = new DDLSource()
                .buildAISFromFile(XXXXXXXX_FK_DDL_FILE_NAME);
        final StringWriter aisw1 = new StringWriter();
        final StringWriter aisw2 = new StringWriter();
        new Writer(new CSVTarget(new PrintWriter(aisw1))).save(ais1);
        new Writer(new CSVTarget(new PrintWriter(aisw2))).save(ais2);
        assertEquals(aisw1.getBuffer().toString(), aisw2.getBuffer().toString());
    }

    @Test
    public void testOverloadedTableColumn() throws Exception {
        String ddl = "CREATE TABLE `s1`.one (idOne int, PRIMARY KEY (idOne)) engine=akibandb;\n"
                + "CREATE TABLE `s2`.one (idTwo int, PRIMARY KEY (idTwo)) engine=akibandb;";

        AkibanInformationSchema ais = new DDLSource().buildAISFromString(ddl);
        assertEquals("user tables", 2, ais.getUserTables().size());
        assertEquals("group tables", 2, ais.getGroupTables().size());
        UserTable s1 = ais.getUserTable("s1", "one");
        UserTable s2 = ais.getUserTable("s2", "one");
        assertNotNull("s1", s1);
        assertNotNull("s2", s2);
        assertSame("s1 group's root", s1, ais.getGroup("one").getGroupTable()
                .getRoot());
        assertSame("s2 group's root", s2, ais.getGroup("one$0").getGroupTable()
                .getRoot());
    }
}
