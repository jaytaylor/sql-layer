/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.CallableStatement;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@RunWith(SelectedParameterizedRunner.class)
public class PostgresServerJDBCFunctionTypesIT extends PostgresServerITBase{

    private final boolean binary;

    @Before
    public void ensureCorrectConnectionType() throws Exception {
        forgetConnection();
    }

    @Override
    protected String getConnectionURL() {
        // loglevel=2 is also useful for seeing what's really happening.
        return super.getConnectionURL() + "?prepareThreshold=1&binaryTransfer=" + binary;
    }

    @Parameterized.Parameters(name="{0}")
    public static Iterable<Object[]> types() throws Exception {
        return Arrays.asList(new Object[]{"Binary", true}, new Object[]{"Text", false});
    }

    public PostgresServerJDBCFunctionTypesIT(String name, boolean binary) {
        this.binary = binary;
    }

    @Before
    public void setupFunctions() throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__stringToInt(a varchar(20)) RETURNS int" +
                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 42 + a.length $$");
        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__intToString(a int) RETURNS varchar(20)" +
                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 'bob' + a $$");
        stmt.close ();
    }

    @After
    public void dropFunctions() throws Exception {
        Statement stmt = getConnection().createStatement();

        stmt.execute("drop FUNCTION testspg__stringToInt");
        stmt.execute("drop FUNCTION testspg__intToString");
        stmt.close ();
    }

    @Test
    public void testStringToInt() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__stringToInt (?) }");
        call.setString (2, "foo");
        call.registerOutParameter (1, Types.INTEGER);
        call.execute ();
        assertEquals(45, call.getInt(1));
    }

    @Test
    public void testIntToString() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__intToString (?) }");
        call.setInt(2, 42);
        call.registerOutParameter (1, Types.VARCHAR);
        call.execute ();
        assertEquals("bob42", call.getString(1));
    }

    @Test
    public void testIntToStringWithDouble() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__intToString (?) }");
        call.setDouble(2, 589.32);
        // JDBC ensures that the OutParameter has the right type
        call.registerOutParameter (1, Types.VARCHAR);
        call.execute ();
        assertEquals("bob589", call.getString(1));
    }

}
