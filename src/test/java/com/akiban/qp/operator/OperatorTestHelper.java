/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.operator;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.persistitadapter.Sorter;
import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import com.persistit.Key;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class OperatorTestHelper {

    // OperatorTestHelper interface

    public static void check(Operator plan, Collection<? extends Row> expecteds, RowCheck additionalCheck) {
        List<Row> actuals = execute(plan);
        if (expecteds.size() != actuals.size()) {
            assertEquals("output", Strings.join(expecteds), Strings.join(actuals));
            assertEquals("size (expecteds=" + expecteds+", actuals=" + actuals + ')', expecteds.size(), actuals.size());
        }
        int rowCount = 0;
        try {
            Iterator<? extends Row> expectedsIter = expecteds.iterator();
            for (Row actual : actuals) {
                Row expected = expectedsIter.next();
                int actualWidth = actual.rowType().nFields();
                assertEquals("row width", expected.rowType().nFields(), actualWidth);
                for (int i = 0; i < actualWidth; ++i) {
                    if (Types3Switch.ON) {
                        checkRowInstance(expected, actual, i, rowCount, actuals, expecteds);
                    } 
                    else {
                        checkRowType(expected, actual, i, rowCount, actuals, expecteds);
                    }
                    
                }
                if (additionalCheck != null)
                    additionalCheck.check(actual);
                ++rowCount;
            }
        }
        finally {
            for (Row actual : actuals) {
                actual.release();
            }
        }
    }
    
    private static void checkRowType(Row expected, Row actual, int i, int rowCount, List<Row> actuals, Collection<? extends Row> expecteds) {
        ValueHolder actualHolder = new ValueHolder(actual.eval(i));
        ValueHolder expectedHolder = new ValueHolder(expected.eval(i));

        if (!expectedHolder.equals(actualHolder)) {
            Assert.assertEquals(
                    String.format("row[%d] field[%d]", rowCount, i),
                    str(expecteds),
                    str(actuals));
            assertEquals(String.format("row[%d] field[%d]", rowCount, i), expectedHolder, actualHolder);
            throw new AssertionError("should have failed by now!");
        }
    }
    
    private static void checkRowInstance(Row expected, Row actual, int i, int rowCount, List<Row> actuals, Collection<? extends Row> expecteds) {   
        PValueSource actualSource = actual.pvalue(i);
        PValueSource expectedSource = expected.pvalue(i);
        TInstance actualType = actual.rowType().typeInstanceAt(i);
        TInstance expectedType = expected.rowType().typeInstanceAt(i);
        if (actualType == null || expectedType == null) {
            assert actualSource.isNull() && expectedSource.isNull();
            return;
        }
        assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));

        
        if(!PValueSources.areEqual(actualSource, expectedSource, expectedType) &&
           !(actualSource.isNull() && expectedSource.isNull())) {
            Assert.assertEquals(
                    String.format("row[%d] field[%d]", rowCount, i),
                    str(expecteds),
                    str(actuals));
            assertEquals(String.format("row[%d] field[%d]", rowCount, i), expectedSource, actualSource);
            throw new AssertionError("should have failed by now!");
        }
    }

    public static void check(Operator plan, Collection<? extends Row> expecteds) {
        check(plan, expecteds, null);
    }

    public static Cursor open(Operator plan) {
        QueryContext queryContext = new SimpleQueryContext(ADAPTER);
        QueryBindings queryBindings = queryContext.createBindings();
        QueryBindingsCursor queryBindingsCursor = new SingletonQueryBindingsCursor(queryBindings);
        Cursor result = plan.cursor(queryContext, queryBindingsCursor);
        reopen(result);
        return result;
    }

    public static void reopen(Cursor cursor) {
        cursor.openTopLevel();
    }

    public static List<Row> execute(Operator plan) {
        List<Row> rows = new ArrayList<>();
        Cursor cursor = open(plan);
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

    public interface RowCheck {
        void check(Row row);
    }

    private static class TestAdapter extends StoreAdapter
    {
        @Override
        public GroupCursor newGroupCursor(Group group)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor newIndexCursor(QueryContext context,
                                     Index index,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IndexScanSelector selector,
                                     boolean usePValues,
                                     boolean openAllSubCursors)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <HKEY extends HKey> HKEY newHKey(com.akiban.ais.model.HKey hKeyMetadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Store getUnderlyingStore() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Row oldRow, Row newRow, boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeRow(Row newRow, Index[] indexes, boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRow(Row oldRow, boolean usePValues, boolean cascadeDefault)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Sorter createSorter(QueryContext context,
                                   QueryBindings bindings,
                           RowCursor input,
                           RowType rowType,
                           API.Ordering ordering,
                           API.SortOption sortOption,
                           InOutTap loadTap)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getQueryTimeoutMilli()
        {
            return -1;
        }

        @Override
        public long rowCount(Session session, RowType tableType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash(ValueSource valueSource, AkCollator collator)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PersistitIndexRow takeIndexRow(IndexRowType indexRowType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void returnIndexRow(PersistitIndexRow indexRow) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IterationHelper createIterationHelper(IndexRowType indexRowType) {
            throw new UnsupportedOperationException();
        }

        public TestAdapter()
        {
            super(null, null, null);
        }

        @Override
        public long sequenceNextValue(TableName sequenceName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long sequenceCurrentValue(TableName sequenceName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Key createKey() {
            throw new UnsupportedOperationException();
        }
    }
}
