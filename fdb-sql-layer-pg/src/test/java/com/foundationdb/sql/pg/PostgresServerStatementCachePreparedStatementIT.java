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

import java.sql.PreparedStatement;
import java.sql.Types;

import org.junit.Before;
import org.junit.Test;
import com.foundationdb.sql.jdbc.PGStatement;

import com.foundationdb.ais.model.TableName;

public class PostgresServerStatementCachePreparedStatementIT extends
        PostgresServerITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "texttable";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);

    @Before
    public void createAndInsert() {
        createTable(TABLE_NAME, "te varchar(100)");
    }

    @Test
    public void testSetNull() throws Exception {
        // valid: fully qualified type to setNull()
        PreparedStatement pstmt = getConnection().prepareStatement("INSERT INTO texttable (te) VALUES (?)");

        if(pstmt instanceof PGStatement) {
            PGStatement pgp = (PGStatement) pstmt;
            pgp.setPrepareThreshold(2);
        }

        pstmt.setNull(1, Types.VARCHAR);
        pstmt.executeUpdate();

        // valid: fully qualified type to setObject()
        pstmt.setObject(1, null, Types.VARCHAR);
        pstmt.executeUpdate();

        // valid: setObject() with partial type info and a typed "null object instance"
        com.foundationdb.sql.jdbc.util.PGobject dummy = new com.foundationdb.sql.jdbc.util.PGobject();
        dummy.setType("VARCHAR");
        dummy.setValue(null);
        pstmt.setObject(1, dummy, Types.OTHER);
        pstmt.executeUpdate();

        // setObject() with no type info
        pstmt.setObject(1, null);
        pstmt.executeUpdate();

        // setObject() with insufficient type info
        pstmt.setObject(1, null, Types.OTHER);
        pstmt.executeUpdate();

        // setNull() with insufficient type info
        pstmt.setNull(1, Types.OTHER);
        pstmt.executeUpdate();

        pstmt.close();
    }
    
}
