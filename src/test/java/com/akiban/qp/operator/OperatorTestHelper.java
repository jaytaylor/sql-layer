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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.Strings;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public final class OperatorTestHelper {

    // OperatorTestHelper interface

    public static void check(Operator plan, Collection<Row> expecteds) {
        List<Row> actuals = execute(plan);
        if (expecteds.size() != actuals.size()) {
            assertEquals("output", Strings.join(expecteds), Strings.join(actuals));
            assertEquals("size (expecteds=" + expecteds+", actuals=" + actuals + ')', expecteds.size(), actuals.size());
        }
        int rowCount = 0;
        try {
            Iterator<Row> expectedsIter = expecteds.iterator();
            for (Row actual : actuals) {
                Row expected = expectedsIter.next();
                for (int i = 0; i < plan.rowType().nFields(); ++i) {
                    ValueHolder actualHolder = new ValueHolder(actual.eval(i));
                    ValueHolder expectedHolder = new ValueHolder(expected.eval(i));

                    if (!expectedHolder.equals(actualHolder)) {
                        Assert.assertEquals(
                                String.format("row[%d] field[%d]", rowCount, i),
                                str(expecteds),
                                str(actuals)
                        );
                        assertEquals(String.format("row[%d] field[%d]", rowCount, i), expectedHolder, actualHolder);
                        throw new AssertionError("should have failed by now!");
                    }
                }
                ++rowCount;
            }
        }
        finally {
            for (Row actual : actuals) {
                actual.release();
            }
        }
    }

    public static List<Row> execute(Operator plan) {
        List<Row> rows = new ArrayList<Row>();
        Cursor cursor = plan.cursor(ADAPTER);
        cursor.open(new ArrayBindings(0));
        try {
            for(Row row = cursor.next(); row != null; row = cursor.next()) {
                row.acquire();
                rows.add(row);
            }
            return rows;
        } finally {
            cursor.close();
        }
    }

    public static Schema schema() {
        return new Schema(new com.akiban.ais.model.AkibanInformationSchema());
    }

    // for use in this class

    private static String str(Collection<? extends Row> rows) {
        return Strings.join(rows);
    }

    private OperatorTestHelper() {}

    // "const"s

    static final TestAdapter ADAPTER = new TestAdapter();

    // nested classes

    private static class TestAdapter extends StoreAdapter
    {
        @Override
        public GroupCursor newGroupCursor(GroupTable groupTable)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor newIndexCursor(Index index, boolean reverse, IndexKeyRange keyRange, IndexScanSelector selector)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public HKey newHKey(RowType rowType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Row oldRow, Row newRow, Bindings bindings)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeRow(Row newRow, Bindings bindings)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRow(Row oldRow, Bindings bindings)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor sort(Cursor input, RowType rowType, API.Ordering ordering, Bindings bindings)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkQueryCancelation(long startTimeMsec)
        {
        }

        @Override
        public long rowCount(RowType tableType)
        {
            throw new UnsupportedOperationException();
        }

        public TestAdapter()
        {
            super(null);
        }
    }
}
