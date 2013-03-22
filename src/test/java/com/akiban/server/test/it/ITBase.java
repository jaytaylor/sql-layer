
package com.akiban.server.test.it;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.geophile.Space;
import com.akiban.server.test.ApiTestBase;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
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
        List<ShareHolder<Row>> actualRows = new ArrayList<>(); // So that result is viewable in debugger
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
                actualRows.add(new ShareHolder<>((Row) actualRow));
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    private boolean equal(RowBase expected, RowBase actual, AkCollator[] collators)
    {
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        if (!equal)
            return false;
        int nFields = actual.rowType().nFields();
        Space space = space(expected.rowType());
        if (space != null) {
            nFields = nFields - space.dimensions() + 1;
        }
        if (usingPValues()) {
            for (int i = 0; i < nFields; i++) {
                PValueSource expectedField = expected.pvalue(i);
                PValueSource actualField = actual.pvalue(i);
                TInstance expectedType = expected.rowType().typeInstanceAt(i);
                TInstance actualType = actual.rowType().typeInstanceAt(i);
                assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));
                int c = TClass.compare(expectedType, expectedField, actualType, actualField);
                if (c != 0)
                    return false;
            }
            return true;
        }
        else {
            ToObjectValueTarget target = new ToObjectValueTarget();
            for (int i = 0; equal && i < nFields; i++) {
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
    }

    private Space space(RowType rowType)
    {
        Space space = null;
        if (rowType instanceof IndexRowType) {
            Index index = ((IndexRowType)rowType).index();
            if (index.isSpatial()) {
                space = index.space();
            }
        }
        return space;
    }

    private AkCollator collator(AkCollator[] collators, int i)
    {
        return
            collators == null ? null :
            collators.length == 0 ? null : collators[i];
    }
}
