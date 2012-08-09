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
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.MultiCursor;
import com.akiban.server.api.dml.ColumnSelector;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiCursorIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void testNoCursors()
    {
        Cursor multiCursor = multiCursor();
        multiCursor.open();
        assertTrue(multiCursor.isIdle());
        assertNull(multiCursor.next());
    }

    @Test
    public void testOneCursor()
    {
        for (int n = 0; n < 10; n++) {
            int[] a = new int[n];
            for (int i = 0; i < n; i++) {
                a[i] = i;
            }
            Cursor multiCursor = multiCursor(new TestCursor(a));
            multiCursor.open();
            assertTrue(multiCursor.isActive());
            Row row;
            long expected = 0;
            while ((row = multiCursor.next()) != null) {
                assertEquals(expected, unwrap(row));
                expected++;
            }
            assertEquals(n, expected);
            assertTrue(multiCursor.isIdle());
        }
    }

    @Test
    public void testMultipleCursors()
    {
        Cursor multiCursor = multiCursor(new TestCursor(new int[]{}),
                                         new TestCursor(new int[]{0, 1, 2}),
                                         new TestCursor(new int[]{}),
                                         new TestCursor(new int[]{}),
                                         new TestCursor(new int[]{3}),
                                         new TestCursor(new int[]{}),
                                         new TestCursor(new int[]{}));
        multiCursor.open();
        Row row;
        long expected = 0;
        while ((row = multiCursor.next()) != null) {
            assertEquals(expected, unwrap(row));
            expected++;
        }
        assertEquals(4, expected);
        assertTrue(multiCursor.isIdle());
    }

    private MultiCursor multiCursor(TestCursor ... cursors)
    {
        MultiCursor multiCursor = new MultiCursor();
        for (TestCursor cursor : cursors) {
            multiCursor.addCursor(cursor);
        }
        return multiCursor;
    }

    private int unwrap(Row row)
    {
        ValuesRow valuesRow = (ValuesRow) row;
        return (int) valuesRow.eval(0).getInt();
    }

    private int t;
    private UserTableRowType tRowType;

    private class TestCursor implements Cursor
    {
        @Override
        public void open()
        {
            position = 0;
        }

        @Override
        public Row next()
        {
            Row row = null;
            if (position < items.length) {
                row = row();
                position++;
            }
            return row;
        }

        @Override
        public void jump(Row row, ColumnSelector columnSelector)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            position = items.length;
        }

        @Override
        public void destroy()
        {
            items = null;
        }

        @Override
        public boolean isIdle()
        {
            return !isDestroyed() && position == items.length;
        }

        @Override
        public boolean isActive()
        {
            return !isDestroyed() && position < items.length;
        }

        @Override
        public boolean isDestroyed()
        {
            return items == null;
        }

        // TestCursor interface

        public TestCursor(int[] items)
        {
            this.items = items;
        }

        // For use by this class

        public Row row()
        {
            return new ValuesRow(tRowType, items[position]);
        }

        // Object state

        private int[] items;
        private int position;
    }
}
