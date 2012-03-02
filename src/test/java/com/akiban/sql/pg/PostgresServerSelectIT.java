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

import com.akiban.sql.NamedParamsTestBase;
import com.akiban.sql.TestBase;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class PostgresServerSelectIT extends PostgresServerFilesITBase 
                                    implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(PostgresServerITBase.RESOURCE_DIR, "select");

    private void createHardCodedTables() {
        // Hack-ish way to create types with types that aren't supported by our SQL
        SimpleColumn columns[] = {
                new SimpleColumn("a_int", "int"), new SimpleColumn("a_uint", "int unsigned"),
                new SimpleColumn("a_float", "float"), new SimpleColumn("a_ufloat", "float unsigned"),
                new SimpleColumn("a_double", "double"), new SimpleColumn("a_udouble", "double unsigned"),
                new SimpleColumn("a_decimal", "decimal", 5L, 2L), new SimpleColumn("a_udecimal", "decimal unsigned", 5L, 2L),
                new SimpleColumn("a_varchar", "varchar", 16L, null), new SimpleColumn("a_date", "date"),
                new SimpleColumn("a_time", "time"), new SimpleColumn("a_datetime", "datetime"),
                new SimpleColumn("a_timestamp", "timestamp"), new SimpleColumn("a_year", "year"),
                new SimpleColumn("a_text", "text")
        };

        createTableFromTypes(SCHEMA_NAME, "types", true, false, columns);
        createTableFromTypes(SCHEMA_NAME, "types_i", true, true, Arrays.copyOf(columns, columns.length - 1));
    }

    @Before
    // Note that this runs _after_ super's openTheConnection(), which
    // means that there is always an AIS generation flush.
    public void loadDatabase() throws Exception {
        createHardCodedTables();
        loadDatabase(RESOURCE_DIR);
    }

    @TestParameters
    public static Collection<Parameterization> queries() throws Exception {
        return NamedParamsTestBase.namedCases(TestBase.sqlAndExpectedAndParams(RESOURCE_DIR));
    }

    public PostgresServerSelectIT(String caseName, String sql, 
                                  String expected, String error,
                                  String[] params) {
        super(caseName, sql, expected, error, params);
    }

    @Test
    public void testQuery() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
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
        ResultSet rs;
        try {
            rs = stmt.executeQuery();
        }
        catch (Exception ex) {
            return ex.getMessage();
        }
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
        return data.toString();
    }

    @Override
    public void checkResult(String result) {
        assertEquals(caseName, expected, result);
    }

}
