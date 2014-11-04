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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresServerJCBCMiscIT extends PostgresServerITBase
{
    @Test
    public void absDecimalResultType() throws Exception {
        Connection conn = getConnection();
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT ABS( CAST('1234.567' AS DECIMAL(12,7)) )");
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("column type", Types.NUMERIC, md.getColumnType(1));
        assertEquals("precision", 12, md.getPrecision(1));
        assertEquals("scale", 7, md.getScale(1));
        assertTrue(rs.next());
        assertEquals("1234.5670000", rs.getString(1));
    }
}
