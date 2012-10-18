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

package com.akiban.server.test.it.qp;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.types.ValueSource;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.*;

public class UniqueIndexUpdateIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "y int",
            "primary key (id)");
        createUniqueIndex("schema", "t", "idx_xy", "x", "y");
        schema = new Schema(ais());
        tRowType = schema.userTableRowType(userTable(t));
        xyIndexRowType = indexType(t, "x", "y");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void testNullSeparatorOnInsert()
    {
        dml().writeRow(session(), createNewRow(t, 1000L, 1L, 1L));
        dml().writeRow(session(), createNewRow(t, 2000L, 2L, 2L));
        dml().writeRow(session(), createNewRow(t, 3000L, 3L, null));
        dml().writeRow(session(), createNewRow(t, 4000L, 4L, null));
        Operator plan = indexScan_Default(xyIndexRowType);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        Row row;
        // Need to examine PIRBs to see null separators
        int count = 0;
        long previousNullSeparator = -1L;
        while ((row = cursor.next()) != null) {
            PersistitIndexRowBuffer indexRow = (PersistitIndexRowBuffer) row;
            long x = indexRow.eval(0).getInt();
            ValueSource yValueSource = indexRow.eval(1);
            long id = indexRow.eval(2).getInt();
            assertEquals(id, x * 1000);
            long nullSeparator = indexRow.nullSeparator();
            if (yValueSource.isNull()) {
                assertTrue(id == 3000 || id == 4000);
                assertTrue(nullSeparator > 0);
                assertTrue(nullSeparator != previousNullSeparator);
            } else {
                assertTrue(id == 1000 || id == 2000);
                assertEquals(0, nullSeparator);
            }
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testNullSeparatorOnUpdate()
    {
        // Load as in testNullSeparatorOnInsert
        dml().writeRow(session(), createNewRow(t, 1000L, 1L, 1L));
        dml().writeRow(session(), createNewRow(t, 2000L, 2L, 2L));
        dml().writeRow(session(), createNewRow(t, 3000L, 3L, null));
        dml().writeRow(session(), createNewRow(t, 4000L, 4L, null));
        // Change nulls to some other value. Scan backwards to avoid halloween issues.
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType, true), queryContext);
        cursor.open();
        Row row;
        final long NEW_Y_VALUE = 99;
        while ((row = cursor.next()) != null) {
            PersistitIndexRowBuffer indexRow = (PersistitIndexRowBuffer) row;
            long x = indexRow.eval(0).getInt();
            long id = indexRow.eval(2).getInt();
            ValueSource yValueSource = indexRow.eval(1);
            if (yValueSource.isNull()) {
                NewRow oldRow = createNewRow(t, id, x, null);
                NewRow newRow = createNewRow(t, id, x, NEW_Y_VALUE);
                dml().updateRow(session(), oldRow, newRow, null);
            }
        }
        cursor.close();
        // Check final state
        cursor = cursor(indexScan_Default(xyIndexRowType), queryContext);
        cursor.open();
        // Need to examine PIRBs to see null separators
        int count = 0;
        while ((row = cursor.next()) != null) {
            PersistitIndexRowBuffer indexRow = (PersistitIndexRowBuffer) row;
            long x = indexRow.eval(0).getInt();
            long y = indexRow.eval(1).getInt();
            long id = indexRow.eval(2).getInt();
            long nullSeparator = indexRow.nullSeparator();
            assertEquals(id, x * 1000);
            assertEquals(0, nullSeparator);
            if (id <= 2000) {
                assertEquals(id, y * 1000);
            } else {
                assertEquals(NEW_Y_VALUE, y);
            }
            count++;
        }
        assertEquals(4, count);
    }

    @Test
    public void testDeleteIndexRowWithNull()
    {
        dml().writeRow(session(), createNewRow(t, 1L, 999L, null));
        dml().writeRow(session(), createNewRow(t, 2L, 999L, null));
        dml().writeRow(session(), createNewRow(t, 3L, 999L, null));
        dml().writeRow(session(), createNewRow(t, 4L, 999L, null));
        dml().writeRow(session(), createNewRow(t, 5L, 999L, null));
        dml().writeRow(session(), createNewRow(t, 6L, 999L, null));
        checkIndex(1, 2, 3, 4, 5, 6);
        // Delete each row
        dml().deleteRow(session(), createNewRow(t, 3L, 999L, null));
        checkIndex(1, 2, 4, 5, 6);
        dml().deleteRow(session(), createNewRow(t, 6L, 999L, null));
        checkIndex(1, 2, 4, 5);
        dml().deleteRow(session(), createNewRow(t, 2L, 999L, null));
        checkIndex(1, 4, 5);
        dml().deleteRow(session(), createNewRow(t, 4L, 999L, null));
        checkIndex(1, 5);
        dml().deleteRow(session(), createNewRow(t, 1L, 999L, null));
        checkIndex(5);
        dml().deleteRow(session(), createNewRow(t, 5L, 999L, null));
        checkIndex();
    }

    private void checkIndex(long ... expectedIds)
    {
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType), queryContext);
        cursor.open();
        Row row;
        int count = 0;
        while ((row = cursor.next()) != null) {
            long id = row.eval(2).getInt();
            assertEquals(expectedIds[count], id);
            count++;
        }
        assertEquals(expectedIds.length, count);
    }

    // Inspired by bug 1036389

    @Test
    public void testUpdateIndexRowWithNull()
    {
        db = new NewRow[]{
            createNewRow(t, 1L, null, null),
        };
        use(db);
        NewRow oldRow = createNewRow(t, 1L, null, null);
        NewRow newRow = createNewRow(t, 1L, 10L, 10L);
        dml().updateRow(session(), oldRow, newRow, null);
        Cursor cursor = cursor(indexScan_Default(xyIndexRowType), queryContext);
        cursor.open();
        Row row = cursor.next();
        assertEquals(10, row.eval(0).getInt());
        assertEquals(10, row.eval(1).getInt());
        assertEquals(1, row.eval(2).getInt());
        row = cursor.next();
        assertNull(row);
    }

    private int t;
    private UserTableRowType tRowType;
    private IndexRowType xyIndexRowType;
}
