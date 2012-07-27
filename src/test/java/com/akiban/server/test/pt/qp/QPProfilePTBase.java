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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.*;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Limit;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitGroupRow;
import com.akiban.qp.persistitadapter.PersistitRowLimit;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.it.qp.TestRow;
import com.akiban.server.test.pt.PTBase;
import com.akiban.server.types.ToObjectValueTarget;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QPProfilePTBase extends PTBase
{
    protected GroupTable groupTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.table().getGroup().getGroupTable();
    }

    protected UserTable userTable(int userTableId)
    {
        RowDef userTableRowDef = rowDefCache().rowDef(userTableId);
        return userTableRowDef.userTable();
    }

    protected IndexRowType indexType(int userTableId, String... searchIndexColumnNamesArray)
    {
        UserTable userTable = userTable(userTableId);
        for (Index index : userTable.getIndexesIncludingInternal()) {
            List<String> indexColumnNames = new ArrayList<String>();
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                indexColumnNames.add(indexColumn.getColumn().getName());
            }
            List<String> searchIndexColumnNames = Arrays.asList(searchIndexColumnNamesArray);
            if (searchIndexColumnNames.equals(indexColumnNames)) {
                return schema.userTableRowType(userTable(userTableId)).indexRowType(index);
            }
        }
        return null;
    }

    protected ColumnSelector columnSelector(final Index index)
    {
        return new ColumnSelector()
        {
            @Override
            public boolean includesColumn(int columnPosition)
            {
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    if (column.getPosition() == columnPosition) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    protected RowBase row(RowType rowType, Object... fields)
    {
        return new TestRow(rowType, fields);
    }

    protected RowBase row(int tableId, Object... values /* alternating field position and value */)
    {
        NiceRow niceRow = new NiceRow(tableId, store());
        int i = 0;
        while (i < values.length) {
            int position = (Integer) values[i++];
            Object value = values[i++];
            niceRow.put(position, value);
        }
        return PersistitGroupRow.newPersistitGroupRow(adapter, niceRow.toRowData());
    }

    protected void compareRows(RowBase[] expected, Cursor cursor)
    {
        List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
        try {
            cursor.open();
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                int count = actualRows.size();
                assertTrue(count < expected.length);
                if(!equal(expected[count], actualRow)) {
                    String expectedString = expected[count] == null ? "null" : expected[count].toString();
                    String actualString = actualRow == null ? "null" : actualRow.toString();
                    assertEquals(expectedString, actualString);
                }
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, actualRows.size());
    }

    protected void compareRenderedHKeys(String[] expected, Cursor cursor)
    {
        int count;
        try {
            cursor.open();
            count = 0;
            List<RowBase> actualRows = new ArrayList<RowBase>(); // So that result is viewable in debugger
            RowBase actualRow;
            while ((actualRow = cursor.next()) != null) {
                assertEquals(expected[count], actualRow.hKey().toString());
                count++;
                actualRows.add(actualRow);
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected.length, count);
    }

    protected boolean equal(RowBase expected, RowBase actual)
    {
        boolean equal = expected.rowType().nFields() == actual.rowType().nFields();
        ToObjectValueTarget target = new ToObjectValueTarget();
        for (int i = 0; equal && i < actual.rowType().nFields(); i++) {
            Object expectedField = target.convertFromSource(expected.eval(i));
            Object actualField = target.convertFromSource(actual.eval(i));
            equal =
                expectedField == actualField || // handles case in which both are null
                expectedField != null && actualField != null && expectedField.equals(actualField);
        }
        return equal;
    }

    protected static final Limit NO_LIMIT = new PersistitRowLimit(ScanLimit.NONE);

    protected Schema schema;
    protected PersistitAdapter adapter;
    protected QueryContext queryContext;
}
