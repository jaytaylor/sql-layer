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

import org.junit.Before;
import org.junit.Test;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import static org.junit.Assert.assertTrue;

public class PostgresServerFormatBinaryOutputIT extends PostgresServerITBase {

    @Before
    public void createTable() throws Exception {
        String createTable = "CREATE TABLE binaryformat (col_int INT PRIMARY KEY NOT NULL, col_binary CHAR(10) FOR BIT DATA, col_varbinary VARCHAR(10) FOR BIT DATA)";
        executeQuery(createTable);
        String insSql = "INSERT INTO binaryformat VALUES (0, x'41', x'42')";
        executeQuery(insSql);
    }

    @Test
    public void testBinaryFormat() throws Exception {

        PreparedStatement getStmt = getConnection().prepareStatement(String.format("SELECT * FROM binaryformat WHERE col_int = 0"));

        String[] setSt = {"SET binary_output TO OCTAL", "SET binary_output TO HEX", "SET binary_output TO BASE64"};
        ResultSet rs;
        String res1, res2, res3;
        for (int i = 0; i < setSt.length; i++) {
            executeQuery(setSt[i]);
            rs = getStmt.executeQuery();
            assertTrue(rs.next());
            res1 = rs.getString(1);
            assertTrue(res1.equals("0"));
            res2 = rs.getString(2);
            res3 = rs.getString(3);
            if ( i == 0) {
                assertTrue(res2.equals("\\101"));
                assertTrue(res3.equals("\\102"));
            } else if ( i == 1) {
                assertTrue(res2.equals("\\x41"));
                assertTrue(res3.equals("\\x42"));
            } else if ( i == 2) {
                assertTrue(res2.equals("QQ=="));
                assertTrue(res3.equals("Qg=="));
            }
            rs.close();
        }
        getStmt.close();
    }

    private void executeQuery(String sql) throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute(sql);
        stmt.close();
    } 
}
