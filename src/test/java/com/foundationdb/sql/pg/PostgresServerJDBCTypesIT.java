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

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;

import java.sql.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test using various JDBC <code>set</code> and <code>get</code> methods.
 */
@RunWith(NamedParameterizedRunner.class)
public class PostgresServerJDBCTypesIT extends PostgresServerITBase
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
            new SimpleColumn("col_timestamp", "MCOMPAT_ timestamp"),
            new SimpleColumn("col_datetime", "MCOMPAT_ datetime"),
        };
        createTableFromTypes(SCHEMA_NAME, "types", false, false, columns);
    }

    static Object[] tc(String name, int jdbcType, String colName, Object value) {
        return new Object[] { name, jdbcType, colName, value };
    }

    @TestParameters
    public static Collection<Parameterization> types() throws Exception {
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
            tc("BigDecimal", Types.DECIMAL, "col_decimal", new BigDecimal("3.14")),
            tc("Boolean", Types.BOOLEAN, "col_boolean", Boolean.TRUE),
            tc("Byte", Types.TINYINT, "col_tinyint", (byte)123),
            tc("Bytes", Types.VARBINARY, "col_varbinary", new byte[] { 0, 1, (byte)0xFF }),
            tc("Date", Types.DATE, "col_date", new Date(startOfDay)),
            tc("Double", Types.DOUBLE, "col_double", 3.14),
            tc("Float", Types.FLOAT, "col_float", 3.14f),
            tc("Int", Types.INTEGER, "col_int", 123456),
            tc("Long", Types.BIGINT, "col_bigint", 0x12345678L),
            tc("Short", Types.SMALLINT, "col_smallint", (short)1001),
            tc("String", Types.VARCHAR, "col_varchar", "hello"),
            tc("Time", Types.TIME, "col_time", new Time(timeOfDay)),
            tc("Timestamp", Types.TIMESTAMP, "col_timestamp", new Timestamp(timeNoMillis)),
            tc("Timestamp(Datetime)", Types.TIMESTAMP, "col_datetime", new Timestamp(timeNoMillis)),
        };
        Collection<Parameterization> result = new ArrayList<>();
        for (Object[] tc : tcs) {
            result.add(Parameterization.create((String)tc[0], tc));
        }
        return result;
    }

    private final String caseName;
    private final int jdbcType;
    private final String colName;
    private final Object value;

    public PostgresServerJDBCTypesIT(String caseName, int jdbcType, String colName,
                                     Object value) {
        this.caseName = caseName;
        this.jdbcType = jdbcType;
        this.colName = colName;
        this.value = value;
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
            assertTrue(Arrays.equals((byte[])expected, (byte[])actual));
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
