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

package com.akiban.sql.pg;
import com.akiban.sql.TestBase;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.io.File;
import java.util.Collection;

@RunWith(Parameterized.class)
public class PostgresServerSelectIT extends PostgresServerITBase
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "select");

    // This cannot be a @Before because that'll run after super's
    // openConnection(), which will then have a stale AIS.
    @Override
    protected void beforeOpenConnection() throws Exception {
        loadDatabase(RESOURCE_DIR);
    }

    @Parameters
    public static Collection<Object[]> queries() throws Exception {
        return TestBase.sqlAndExpectedAndParams(RESOURCE_DIR);
    }

    public PostgresServerSelectIT(String caseName, String sql, String expected, 
                                  String[] params) {
        super(caseName, sql, expected, params);
    }

    @Test
    public void testQuery() throws Exception {
        StringBuilder data = new StringBuilder();
        PreparedStatement stmt = connection.prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String param = params[i];
                if (param.startsWith("#"))
                    stmt.setLong(i + 1, Long.parseLong(param.substring(1)));
                else
                    stmt.setString(i + 1, param);
            }
        }
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if (i > 1) data.append('\t');
            data.append(md.getColumnName(i));
        }
        data.append('\n');
        while (rs.next()) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) data.append('\t');
                data.append(rs.getString(i));
            }
            data.append('\n');
        }
        stmt.close();
        assertEquals("Difference in " + caseName, expected, data.toString());
    }
}
