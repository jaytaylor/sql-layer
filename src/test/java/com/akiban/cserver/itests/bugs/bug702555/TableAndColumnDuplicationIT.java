package com.akiban.cserver.itests.bugs.bug702555;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.itests.ApiTestBase;
import org.junit.Test;

public final class TableAndColumnDuplicationIT extends ApiTestBase {

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
        final TableId schema1Table
                = createTable("schema1", "table1", "id int key");
        final TableId schema2Table =
                createTable("schema2","table1", "name varchar(32) key");

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
        final TableId schema1Table
                = createTable("schema1", schema1TableName, schema1TableKeyCol + " int key, name varchar(32)");
        final TableId schema2Table =
                createTable("schema2", schema2TableName, schema2TableKeyCol + " int key, name varchar(32)");

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
