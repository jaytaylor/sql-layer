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

package com.akiban.server.test.it;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.server.collation.AkCollator;
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
        compareRows(expected, cursor, null);
    }

    protected void compareRows(RowBase[] expected, Cursor cursor, AkCollator ... collators)
    {
        List<ShareHolder<Row>> actualRows = new ArrayList<ShareHolder<Row>>(); // So that result is viewable in debugger
        try {
            cursor.open();
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(String.format("failed test %d < %d (more rows than expected)", count, expected.length), count < expected.length);
                if(!equal(expected[count], actualRow, collators)) {
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

    protected boolean equal(RowBase expected, RowBase actual, AkCollator[] collators)
    {
        ToObjectValueTarget target = new ToObjectValueTarget();
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        for (int i = 0; equal && i < actual.rowType().nFields(); i++) {
            Object expectedField = target.convertFromSource(expected.eval(i));
            Object actualField = target.convertFromSource(actual.eval(i));
            if (expectedField == null && actualField == null) {
                equal = true;
            } else if (expectedField == null || actualField == null) {
                equal = false;
            } else if (collator(collators, i) != null &&
                       expectedField instanceof String &&
                       actualField instanceof String) {
                collator(collators, i).compare((String) expectedField, (String) actualField);
            } else {
                equal = expectedField.equals(actualField);
            }
        }
        return equal;
    }

    private AkCollator collator(AkCollator[] collators, int i)
    {
        return
            collators == null ? null :
            collators.length == 0 ? null : collators[i];
    }
}
