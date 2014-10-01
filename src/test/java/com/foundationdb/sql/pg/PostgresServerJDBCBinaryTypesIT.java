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


import com.foundationdb.junit.SelectedParameterizedRunner;
import com.foundationdb.sql.jdbc.util.PSQLException;
import com.foundationdb.sql.jdbc.util.PSQLState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test to check that we handle negative options on binary encoded values
 * (e.g. sending a short, when a long is expected)
 *
 * Note: only some types are binary encoded when passing to the server,
 * for example, at the time of this writing, setBoolean does a text transfer
 * with "0" or "1"
 */
public class PostgresServerJDBCBinaryTypesIT extends PostgresServerITBase
{

    @Before
    public void createTable() throws Exception {
        // Most of these aren't exercised because JDBC only sends integers and float/double as binary over pg.
        SimpleColumn columns[] = {
            new SimpleColumn("col_boolean", "AKSQL_ boolean"),
            new SimpleColumn("col_tinyint", "MCOMPAT_ tinyint"),
            new SimpleColumn("col_varbinary", "MCOMPAT_ varbinary", 256L, null),
            new SimpleColumn("col_date", "MCOMPAT_ date"),
            new SimpleColumn("col_decimal", "MCOMPAT_ decimal", 5L, 2L),
            new SimpleColumn("col_double", "MCOMPAT_ double"),
            new SimpleColumn("col_float", "MCOMPAT_ float"),
            new SimpleColumn("col_int", "MCOMPAT_ int"),
            new SimpleColumn("col_bigint", "MCOMPAT_ bigint"),
            new SimpleColumn("col_smallint", "MCOMPAT_ smallint"),
            new SimpleColumn("col_varchar", "MCOMPAT_ varchar", 16L, null),
            new SimpleColumn("col_time", "MCOMPAT_ time"),
            new SimpleColumn("col_datetime", "MCOMPAT_ datetime"),
            new SimpleColumn("col_guid", "AKSQL_ GUID"),
        };
        createTableFromTypes(SCHEMA_NAME, "types", false, false, columns);
    }

    private PreparedStatement setStmt(String columnName) throws Exception {
        return getConnection().prepareStatement(String.format("INSERT INTO types(id,%s) VALUES(1,?)", columnName));
    }

    private void checkValue(String columnName, Object value, int jdbcType) throws Exception {
        PreparedStatement getStmt = getConnection().prepareStatement(
                String.format("SELECT %s FROM types WHERE id = ?", columnName));
        getStmt.setInt(1, 1);
        ResultSet rs = getStmt.executeQuery();
        assertTrue(rs.next());
        compareObjects(asObject(value, jdbcType), rs.getObject(1));
        rs.close();
    }

    @Test
    public void testSetLongWithShort() throws Exception {
        PreparedStatement set = setStmt("col_bigint");
        set.setShort(1, (short)34);
        set.executeUpdate();
        checkValue("col_bigint", 34L, Types.BIGINT);
    }

    @Test
    public void testSetShortWithLong() throws Exception {
        PreparedStatement set = setStmt("col_smallint");
        set.setLong(1, 34L);
        set.executeUpdate();
        checkValue("col_smallint", (short)34, Types.SMALLINT);
    }

    @Test
    public void testSetShortWithTooBigLong() throws Exception {
        PreparedStatement set = setStmt("col_smallint");
        set.setLong(1, 342147483641L);
        set.executeUpdate();
        // It looks like the code currently does a cast, so (short)longValue
        // The key for right now is to not crash.
        checkValue("col_smallint", (short)18425, Types.SMALLINT);
    }

    @Test
    public void testSetShortWithDouble() throws Exception {
        PreparedStatement set = setStmt("col_smallint");
        set.setDouble(1, 34);
        set.executeUpdate();
        checkValue("col_smallint", (short)34, Types.SMALLINT);
    }

    @Test
    public void testSetShortWithTooBigDouble() throws Exception {
        PreparedStatement set = setStmt("col_smallint");
        set.setDouble(1, 34.33E128);
        set.executeUpdate();
        // Double's aren't integers, so it parses the double, then calls longValue(), which clamps, then casts
        // which results in -1. The key for right now is to not crash.
        checkValue("col_smallint", (short)-1, Types.SMALLINT);
    }

    @Test
    public void testSetDoubleWithShort() throws Exception {
        PreparedStatement set = setStmt("col_double");
        set.setShort(1, (short)52);
        set.executeUpdate();
        checkValue("col_double", 52.0, Types.DOUBLE);
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + true;
    }

    protected static Object asObject(Object value, int jdbcType) {
        switch (jdbcType) {
        case Types.TINYINT:
            return ((Byte)value).intValue();
        case Types.SMALLINT:
            return ((Short)value).intValue();
        default:
            return value;
        }
    }

    protected static void compareObjects(Object expected, Object actual) {
        if (expected instanceof byte[]) {
            assertTrue("Expected "  + Arrays.toString((byte[])expected) + " but got " + Arrays.toString((byte[])actual),
                    Arrays.equals((byte[])expected, (byte[])actual));
        }
        else if (expected instanceof java.util.Date) {
            assertEquals(String.format("%s <> %s", 
                                       ((java.util.Date)expected).getTime(),
                                       ((java.util.Date)actual).getTime()),
                         expected, actual);
        }
        else {
            assertEquals(expected, actual);
        }
    }

}
