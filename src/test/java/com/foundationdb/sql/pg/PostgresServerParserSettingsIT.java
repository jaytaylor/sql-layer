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

package com.foundationdb.sql.pg;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresServerParserSettingsIT extends PostgresServerITBase
{
    private static final String CONFIG_PREFIX = "fdbsql.postgres.";
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String,String> settings = new HashMap<>(super.startupConfigProperties());
        // Startup time only config settings
        settings.put(CONFIG_PREFIX + "columnAsFunc", "true");
        settings.put(CONFIG_PREFIX + "parserDoubleQuoted", "string");
        settings.put(CONFIG_PREFIX + "parserInfixBit", "true");
        settings.put(CONFIG_PREFIX + "parserInfixLogical", "true");
        return settings;
    }

    @Test
    public void parserSettings() throws Exception {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        ResultSet rs;
        ResultSetMetaData md;

        // special-column as functions
        rs = s.executeQuery("SELECT CURRENT_DATE, CURRENT_DATE(), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP(), CURRENT_TIME, CURRENT_TIME()");
        md = rs.getMetaData();
        assertEquals(6, md.getColumnCount());
        assertEquals(Types.DATE, md.getColumnType(1));
        assertEquals(Types.DATE, md.getColumnType(2));
        assertEquals(Types.TIMESTAMP, md.getColumnType(3));
        assertEquals(Types.TIMESTAMP, md.getColumnType(4));
        assertEquals(Types.TIME, md.getColumnType(5));
        assertEquals(Types.TIME, md.getColumnType(6));

        // double quote as string
        rs = s.executeQuery("SELECT \"foo\"");
        assertTrue(rs.next());
        assertEquals("foo", rs.getString(1));

        // infix bit operators
        rs = s.executeQuery("SELECT 1|2");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));

        // infix logical operators
        rs = s.executeQuery("SELECT true||false");
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
    }
}
