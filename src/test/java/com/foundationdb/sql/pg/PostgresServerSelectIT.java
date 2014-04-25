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

import com.foundationdb.sql.NamedParamsTestBase;
import com.foundationdb.sql.TestBase;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
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
            new SimpleColumn("a_int", "MCOMPAT_ int"), new SimpleColumn("a_uint", "MCOMPAT_ int unsigned"),
            new SimpleColumn("a_float", "MCOMPAT_ float"), new SimpleColumn("a_ufloat", "MCOMPAT_ float unsigned"),
            new SimpleColumn("a_double", "MCOMPAT_ double"), new SimpleColumn("a_udouble", "MCOMPAT_ double unsigned"),
            new SimpleColumn("a_decimal", "MCOMPAT_ decimal", 5L, 2L), new SimpleColumn("a_udecimal", "MCOMPAT_ decimal unsigned", 5L, 2L),
            new SimpleColumn("a_varchar", "MCOMPAT_ varchar", 16L, null), new SimpleColumn("a_date", "MCOMPAT_ date"),
            new SimpleColumn("a_time", "MCOMPAT_ time"), new SimpleColumn("a_datetime", "MCOMPAT_ datetime"),
            new SimpleColumn("a_timestamp", "MCOMPAT_ timestamp"), new SimpleColumn("a_year", "MCOMPAT_ year"),
            new SimpleColumn("a_text", "MCOMPAT_ text")
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
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String param = params[i];
                if (param.startsWith("%")) {
                    switch (param.charAt(1)) {
                    case 'B':
                        stmt.setBoolean(i + 1, Boolean.parseBoolean(param.substring(2)));
                        break;
                    case 'b':
                        stmt.setByte(i + 1, Byte.parseByte(param.substring(2)));
                        break;
                    case 's':
                        stmt.setShort(i + 1, Short.parseShort(param.substring(2)));
                        break;
                    case 'i':
                        stmt.setInt(i + 1, Integer.parseInt(param.substring(2)));
                        break;
                    case 'l':
                        stmt.setLong(i + 1, Long.parseLong(param.substring(2)));
                        break;
                    case 'f':
                        stmt.setFloat(i + 1, Float.parseFloat(param.substring(2)));
                        break;
                    case 'd':
                        stmt.setDouble(i + 1, Double.parseDouble(param.substring(2)));
                        break;
                    case 'n':
                        stmt.setBigDecimal(i + 1, new java.math.BigDecimal(param.substring(2)));
                        break;
                    case 'D':
                        stmt.setDate(i + 1, java.sql.Date.valueOf(param.substring(2)));
                        break;
                    case 't':
                        stmt.setTime(i + 1, java.sql.Time.valueOf(param.substring(2)));
                        break;
                    case 'T':
                        stmt.setTimestamp(i + 1, java.sql.Timestamp.valueOf(param.substring(2)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type prefix " + param);
                    }
                }
                else
                    stmt.setString(i + 1, param);
            }
        }
        ResultSet rs;
        try {
            rs = stmt.executeQuery();
            if (executeTwice()) {
                rs.close();
                rs = stmt.executeQuery();
            }
        }
        catch (Exception ex) {
            if (error == null)
                forgetConnection();
            throw ex;
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

    protected boolean executeTwice() {
        return false;
    }

}
