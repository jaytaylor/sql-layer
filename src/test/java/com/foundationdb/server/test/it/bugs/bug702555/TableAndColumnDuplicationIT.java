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

package com.foundationdb.server.test.it.bugs.bug702555;

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

public final class TableAndColumnDuplicationIT extends ITBase {

    @Test
    public void sameTableAndColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl1", "id1");
    }

    @Test
    public void sameTableDifferentColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl1", "id2");
    }

    @Test
    public void differentTableSameColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl2", "id1");
    }

    @Test
    public void differentTableDifferentColumn() throws InvalidOperationException {
        doTest( "tbl1", "id1",
                "tbl2", "id2");
    }

    /**
     * A potentially more subtle problem. No duplicate key exceptions are thrown, because the two tables have
     * inherently incompatible primary keys. But data gets written to the same tree, and thus a read stumbles
     * across rows it shouldn't see
     * @throws InvalidOperationException if any CRUD operation fails
     */
    @Test
    public void noDuplicateKeyButIncompatibleRows() throws InvalidOperationException {
        final int schema1Table
                = createTable("schema1", "table1", "id int not null primary key");
        final int schema2Table =
                createTable("schema2","table1", "name varchar(32) not null primary key");

        writeRows(
                row(schema1Table, 0),
                row(schema1Table, 1),
                row(schema1Table, 2)
        );

        writeRows(
                row(schema2Table, "first row"),
                row(schema2Table, "second row"),
                row(schema2Table, "third row")
        );

        expectFullRows(schema1Table,
                row(schema1Table, 0),
                row(schema1Table, 1),
                row(schema1Table, 2)
        );
        
        expectFullRows(schema2Table,
                row(schema2Table, "first row"),
                row(schema2Table, "second row"),
                row(schema2Table, "third row")
        );
    }

    private void doTest(String schema1TableName, String schema1TableKeyCol,
                        String schema2TableName, String schema2TableKeyCol) throws InvalidOperationException
    {
        final int schema1Table
                = createTable("schema1", schema1TableName, schema1TableKeyCol + " int not null primary key, name varchar(32)");
        final int schema2Table =
                createTable("schema2", schema2TableName, schema2TableKeyCol + " int not null primary key, name varchar(32)");

        writeRows(
                row(schema1Table, 0, "alpha-0"),
                row(schema1Table, 1, "alpha-1"),
                row(schema1Table, 2, "alpha-1")
        );

        writeRows(
                row(schema2Table, 0, "bravo-0"),
                row(schema2Table, 1, "bravo-1"),
                row(schema2Table, 2, "bravo-1")
        );

        expectFullRows( schema1Table,
                row(schema1Table, 0, "alpha-0"),
                row(schema1Table, 1, "alpha-1"),
                row(schema1Table, 2, "alpha-1")
        );

        expectFullRows( schema2Table,
                row(schema2Table, 0, "bravo-0"),
                row(schema2Table, 1, "bravo-1"),
                row(schema2Table, 2, "bravo-1")
        );
    }
}
