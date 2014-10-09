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

import java.sql.*;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;

import com.foundationdb.sql.jdbc.util.PSQLState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test using various JDBC <code>set</code> and <code>get</code> methods.
 */
@RunWith(SelectedParameterizedRunner.class)
public abstract class PostgresServerJDBCTypesITBase extends PostgresServerITBase
{

    @Before
    public void createTable() throws Exception {
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

    @Before
    public void ensureCorrectConnectionType() throws Exception {
        forgetConnection();
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + binaryTransfer();
    }

    protected abstract boolean binaryTransfer();

    /**
     * @param unparseable some sort of string that couldn't possibly be parsed (e.g. an int of value "Suzie")
     * @param defaultValue the default value that should come back if you pass in a wildly incorrect string
     */
    static Object[] tc(String name, int jdbcType, String colName, Object value,
                       String unparseable, Object defaultValue) {
        return new Object[] { name, jdbcType, colName, value, unparseable, defaultValue};
    }

    @Parameters(name="{0}")
    public static Iterable<Object[]> types() throws Exception {
        Calendar cal = new GregorianCalendar();
        cal.set(Calendar.MILLISECOND, 0);
        long timeNoMillis = cal.getTime().getTime();
        Calendar dcal = (Calendar)cal.clone();
        dcal.set(Calendar.HOUR_OF_DAY, 0);
        dcal.set(Calendar.MINUTE, 0);
        dcal.set(Calendar.SECOND, 0);
        long startOfDay = dcal.getTime().getTime();
        Calendar tcal = (Calendar)cal.clone();
        tcal.set(Calendar.YEAR, 1970);
        tcal.set(Calendar.MONTH, 0);
        tcal.set(Calendar.DAY_OF_MONTH, 1);
        long timeOfDay = tcal.getTime().getTime();
        Object[][] tcs = new Object[][] {
            tc("BigDecimal", Types.DECIMAL, "col_decimal", new BigDecimal("3.14"), "Suzie", new BigDecimal("0.00")),
            tc("Boolean", Types.BOOLEAN, "col_boolean", Boolean.TRUE, "Jack", false),
            tc("Byte", Types.TINYINT, "col_tinyint", (byte)123, "Lewis", (byte)0),
            // strings are parsed into byte arrays
            tc("Bytes", Types.VARBINARY, "col_varbinary", new byte[] { 0, 1, (byte)0xFF }, null, null),
            tc("Date", Types.DATE, "col_date", new Date(startOfDay), "Janet", null),
            tc("Double", Types.DOUBLE, "col_double", 3.14E52, "Bridget", 0.0),
            tc("Float", Types.FLOAT, "col_float", 3.14f, "Willy", 0.0f),
            tc("Int", Types.INTEGER, "col_int", 123456, "Mary", 0),
            tc("Long", Types.BIGINT, "col_bigint", 0x12345678L, "Jimmy", 0L),
            tc("Short", Types.SMALLINT, "col_smallint", (short)1001, "Martha", (short)0),
            // obviously any string can be a string
            tc("String", Types.VARCHAR, "col_varchar", "hello", null, null),
            tc("Time", Types.TIME, "col_time", new Time(timeOfDay), "Mike", null),
            tc("Timestamp(Datetime)", Types.TIMESTAMP, "col_datetime", new Timestamp(timeNoMillis), "Bob", null),
            tc("GUID", Types.OTHER, "col_guid", UUID.randomUUID(), "3249",
               new PSQLException("3249", new PSQLState("2202I"))),
        };
        return Arrays.asList(tcs);
    }

    private final String caseName;
    private final int jdbcType;
    private final String colName;
    private final Object value;
    private final String unparseable;
    private final Object defaultValue;

    public PostgresServerJDBCTypesITBase(String caseName, int jdbcType, String colName,
                                         Object value, String unparseable, Object defaultValue) {
        this.caseName = caseName;
        this.jdbcType = jdbcType;
        this.colName = colName;
        this.value = value;
        this.unparseable = unparseable;
        this.defaultValue = defaultValue;
    }

    @Test
    public void setAndGet() throws Exception {
        PreparedStatement setStmt = getConnection().prepareStatement(String.format("INSERT INTO types(id,%s) VALUES(?,?)", colName));
        PreparedStatement getStmt = getConnection().prepareStatement(String.format("SELECT %s FROM types WHERE id = ?", colName));

        setStmt.setInt(1, 1);
        setStmt.setObject(2, value, jdbcType);
        setStmt.executeUpdate();
        getStmt.setInt(1, 1);
        ResultSet rs = getStmt.executeQuery();
        assertTrue(rs.next());
        compareObjects(asObject(value, jdbcType), rs.getObject(1));
        rs.close();
        
        setStmt.setInt(1, 2);
        setMethod(setStmt, 2, value, jdbcType);
        setStmt.executeUpdate();
        getStmt.setInt(1, 2);
        rs = getStmt.executeQuery();
        assertTrue(rs.next());
        compareObjects(value, getMethod(rs, 1, jdbcType));
        rs.close();
        
        getStmt.close();
        setStmt.close();
    }

    @Test
    public void setAsString() throws Exception {
        PreparedStatement setStmt = getConnection().prepareStatement(String.format("INSERT INTO types(id,%s) VALUES(?,?)", colName));
        PreparedStatement getStmt = getConnection().prepareStatement(String.format("SELECT %s FROM types WHERE id = ?", colName));
        Object valueForStrings = value;
        // "3true" -> true
        // check out Date with "3"+value.toString()
        // also Date/Time with value.toString() + "3" -> get's parsed as null
        // bytes need special handling here.
        if (value instanceof byte[]) {
            byte[] bytes = (byte[])value;
            byte[] bytesCopy = new byte[bytes.length];
            valueForStrings = bytesCopy;
            // large bytes fall over when you try to encode them as UTF-8
            // not going to worry about that here
            for (int i=0; i<bytes.length; i++) {
                if (bytes[i] < 0) {
                    bytesCopy[i] = 37;
                } else {
                    bytesCopy[i] = bytes[i];
                }
            }
            setStmt.setString(2, new String(bytesCopy, "UTF-8"));
        } else {
            setStmt.setString(2, valueForStrings.toString());
        }
        setStmt.setInt(1, 1);
        setStmt.executeUpdate();
        getStmt.setInt(1, 1);
        ResultSet rs = getStmt.executeQuery();
        assertTrue(rs.next());
        compareObjects(asObject(valueForStrings, jdbcType), rs.getObject(1));
        rs.close();

        getStmt.close();
        setStmt.close();
    }

    @Test
    public void setUnparseableString() throws Exception {
        PreparedStatement setStmt = getConnection().prepareStatement(String.format("INSERT INTO types(id,%s) VALUES(?,?)", colName));
        PreparedStatement getStmt = getConnection().prepareStatement(String.format("SELECT %s FROM types WHERE id = ?", colName));
        setStmt.setString(2, unparseable);
        setStmt.setInt(1, 1);
        if (defaultValue instanceof Exception) {
            try {
                setStmt.executeUpdate();
                fail("Expected an exception to be thrown");
            } catch (Exception e) {
                assertThat(e, is(instanceOf(defaultValue.getClass())));
                assertThat(e.getMessage(), containsString(((Exception) defaultValue).getMessage()));
                if (defaultValue instanceof PSQLException) {
                    assertEquals(((PSQLException)defaultValue).getSQLState(),
                            ((PSQLException)e).getSQLState());
                }
            }
        } else {
            setStmt.executeUpdate();
            getStmt.setInt(1, 1);
            ResultSet rs = getStmt.executeQuery();
            assertTrue(rs.next());
            compareObjects(asObject(defaultValue, jdbcType), rs.getObject(1));
            rs.close();

            getStmt.close();
        }
        setStmt.close();
    }

    protected static void setMethod(PreparedStatement stmt, int index,
                                    Object value, int jdbcType) 
            throws Exception {
        switch (jdbcType) {
        case Types.DECIMAL:
            stmt.setBigDecimal(index, (BigDecimal)value);
            break;
        case Types.BOOLEAN:
            stmt.setBoolean(index, (Boolean)value);
            break;
        case Types.TINYINT:
            stmt.setByte(index, (Byte)value);
            break;
        case Types.VARBINARY:
            stmt.setBytes(index, (byte[])value);
            break;
        case Types.DATE:
            stmt.setDate(index, (Date)value);
            break;
        case Types.DOUBLE:
            stmt.setDouble(index, (Double)value);
            break;
        case Types.FLOAT:
            stmt.setFloat(index, (Float)value);
            break;
        case Types.INTEGER:
            stmt.setInt(index, (Integer)value);
            break;
        case Types.BIGINT:
            stmt.setLong(index, (Long)value);
            break;
        case Types.SMALLINT:
            stmt.setShort(index, (Short)value);
            break;
        case Types.VARCHAR:
            stmt.setString(index, (String)value);
            break;
        case Types.TIME:
            stmt.setTime(index, (Time)value);
            break;
        case Types.TIMESTAMP:
            stmt.setTimestamp(index, (Timestamp)value);
            break;
        case Types.OTHER:
            stmt.setObject(index, value);
            break;
        default:
            fail("Unknown JDBC type");
        }
    }

    protected static Object getMethod(ResultSet rs, int index, int jdbcType) 
            throws Exception {
        switch (jdbcType) {
        case Types.DECIMAL:
            return rs.getBigDecimal(index);
        case Types.BOOLEAN:
            return rs.getBoolean(index);
        case Types.TINYINT:
            return rs.getByte(index);
        case Types.VARBINARY:
            return rs.getBytes(index);
        case Types.DATE:
            return rs.getDate(index);
        case Types.DOUBLE:
            return rs.getDouble(index);
        case Types.FLOAT:
            return rs.getFloat(index);
        case Types.INTEGER:
            return rs.getInt(index);
        case Types.BIGINT:
            return rs.getLong(index);
        case Types.SMALLINT:
            return rs.getShort(index);
        case Types.VARCHAR:
            return rs.getString(index);
        case Types.TIME:
            return rs.getTime(index);
        case Types.TIMESTAMP:
            return rs.getTimestamp(index);
        case Types.OTHER:
            return rs.getObject(index);
        default:
            fail("Unknown JDBC type");
            return null;
        }
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
            assertTrue("Expected "  + Arrays.toString((byte[])expected) + " but got " + Arrays.toString((byte[])actual), Arrays.equals((byte[])expected, (byte[])actual));
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
