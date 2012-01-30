/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.server.test.ApiTestBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.util.ShareHolder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class ITBase extends ApiTestBase {
    public ITBase() {
        super("IT");
    }

    protected ITBase(String suffix) {
        super(suffix);
    }

    protected void compareRows(RowBase[] expected, Cursor cursor)
    {
        List<ShareHolder<Row>> actualRows = new ArrayList<ShareHolder<Row>>(); // So that result is viewable in debugger
        try {
            cursor.open();
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(String.format("failed test %d < %d", count, expected.length), count < expected.length);
                if(!equal(expected[count], actualRow)) {
                    String expectedString = expected[count] == null ? "null" : expected[count].toString();
                    String actualString = actualRow == null ? "null" : actualRow.toString();
                    assertEquals("row " + count, expectedString, actualString);
                }
                if (expected[count] instanceof TestRow) {
                    TestRow expectedTestRow = (TestRow) expected[count];
                    if (expectedTestRow.persistityString() != null) {
                        String actualHKeyString = actualRow == null ? "null" : actualRow.hKey().toString();
                        assertEquals(count + ": hkey", expectedTestRow.persistityString(), actualHKeyString);
                    }
                }
                actualRows.add(new ShareHolder<Row>((Row) actualRow));
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    protected boolean equal(RowBase expected, RowBase actual)
    {
        ToObjectValueTarget target = new ToObjectValueTarget();
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        for (int i = 0; equal && i < actual.rowType().nFields(); i++) {
            Object expectedField = target.convertFromSource(expected.eval(i));
            Object actualField = target.convertFromSource(actual.eval(i));
            equal =
                expectedField == actualField || // handles case in which both are null
                expectedField != null && actualField != null && expectedField.equals(actualField);
        }
        return equal;
    }
}
