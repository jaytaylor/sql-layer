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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DDLSourceTest {

    // Last method of DDLSource in usage (by studio)
    @Test
    public void testCreateTable() throws Exception {
        DDLSource ddlSource = new DDLSource();
        final SchemaDef.UserTableDef akibandbUT = ddlSource.parseCreateTable("t(id int, name varchar(32), key(id))");
        assertEquals("akibandb", akibandbUT.engine);
        assertEquals(3, akibandbUT.columns.size());     // id, name, <hidden pk>
        assertEquals(1, akibandbUT.primaryKey.size());  // hidden pk
        assertEquals(1, akibandbUT.indexes.size());
        assertNull(akibandbUT.getCName().getSchema());
        assertEquals("t", akibandbUT.getCName().getName());

        final SchemaDef.UserTableDef innodbUT = ddlSource.parseCreateTable("x.y(value int, primary key(id)) engine=innodb");
        assertEquals("innodb", innodbUT.engine);
        assertEquals(1, innodbUT.columns.size());
        assertEquals(1, innodbUT.primaryKey.size());
        assertEquals(0, innodbUT.indexes.size());
        assertEquals("x", innodbUT.getCName().getSchema());
        assertEquals("y", innodbUT.getCName().getName());
    }
}
