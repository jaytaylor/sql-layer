/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.bugs.bug702555;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
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
                createNewRow(schema1Table, 0L),
                createNewRow(schema1Table, 1L),
                createNewRow(schema1Table, 2L)
        );

        writeRows(
                createNewRow(schema2Table, "first row"),
                createNewRow(schema2Table, "second row"),
                createNewRow(schema2Table, "third row")
        );

        expectFullRows(schema1Table,
                createNewRow(schema1Table, 0L),
                createNewRow(schema1Table, 1L),
                createNewRow(schema1Table, 2L)
        );
        
        expectFullRows(schema2Table,
                createNewRow(schema2Table, "first row"),
                createNewRow(schema2Table, "second row"),
                createNewRow(schema2Table, "third row")
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
                createNewRow(schema1Table, 0L, "alpha-0"),
                createNewRow(schema1Table, 1L, "alpha-1"),
                createNewRow(schema1Table, 2L, "alpha-1")
        );

        writeRows(
                createNewRow(schema2Table, 0L, "bravo-0"),
                createNewRow(schema2Table, 1L, "bravo-1"),
                createNewRow(schema2Table, 2L, "bravo-1")
        );

        expectFullRows( schema1Table,
                createNewRow(schema1Table, 0L, "alpha-0"),
                createNewRow(schema1Table, 1L, "alpha-1"),
                createNewRow(schema1Table, 2L, "alpha-1")
        );

        expectFullRows( schema2Table,
                createNewRow(schema2Table, 0L, "bravo-0"),
                createNewRow(schema2Table, 1L, "bravo-1"),
                createNewRow(schema2Table, 2L, "bravo-1")
        );
    }
}
