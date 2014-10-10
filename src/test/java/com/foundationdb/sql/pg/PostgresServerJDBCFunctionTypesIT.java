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
                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 42 $$");
//        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__getString(a varchar(20)) RETURNS VARCHAR(20)" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 'bob' $$");
//        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__getDouble(a double) RETURNS double" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS $$ 42.42 $$");
//        stmt.execute ("CREATE OR REPLACE PROCEDURE testspg__getVoid (IN a float) " +
//                " LANGUAGE javascript PARAMETER STYLE variables AS ''");
//
//        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__getInt (a int) RETURNS int" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS '42'");
//        stmt.execute("CREATE OR REPLACE FUNCTION testspg__getShort (a smallint) RETURNS smallint" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS '42'");
//        stmt.execute ("CREATE OR REPLACE FUNCTION testspg__getNumeric (a numeric) RETURNS numeric" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS '42'");
//
//        stmt.execute("CREATE OR REPLACE FUNCTION testspg__getNumericWIthoutArg() RETURNS numeric" +
//                " LANGUAGE javascript PARAMETER STYLE variables AS '42'");

        stmt.close ();
    }

    @After
    public void dropFunctions() throws Exception {
        Statement stmt = getConnection().createStatement();

        stmt.execute("drop FUNCTION testspg__stringToInt");
//        stmt.execute("drop FUNCTION testspg__getString");
//        stmt.execute("drop FUNCTION testspg__getDouble");
//        stmt.execute("drop PROCEDURE testspg__getVoid");
//        stmt.execute("drop FUNCTION testspg__getInt");
//        stmt.execute("drop FUNCTION testspg__getShort");
//        stmt.execute("DROP FUNCTION testspg__getNumeric");
//        stmt.execute("drop FUNCTION testspg__getNumericWithoutArg");

        stmt.close ();
    }

    @Test
    public void testStringToInt() throws Exception {
        CallableStatement call = getConnection().prepareCall("{ ? = call testspg__stringToInt (?) }");
        call.setString (2, "foo");
        call.registerOutParameter (1, Types.INTEGER);
        call.execute ();
        assertEquals(42, call.getInt(1));
    }
}
