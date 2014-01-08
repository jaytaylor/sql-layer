/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.storeadapter.Sorter;
import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.Strings;
import com.foundationdb.util.tap.InOutTap;
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
        Iterator<? extends Row> expectedsIter = expecteds.iterator();
        for (Row actual : actuals) {
            Row expected = expectedsIter.next();
            int actualWidth = actual.rowType().nFields();
            assertEquals("row width", expected.rowType().nFields(), actualWidth);
            for (int i = 0; i < actualWidth; ++i) {
                checkRowInstance(expected, actual, i, rowCount, actuals, expecteds);
            }
            if (additionalCheck != null)
                additionalCheck.check(actual);
            ++rowCount;
        }
   }
    
    private static void checkRowInstance(Row expected, Row actual, int i, int rowCount, List<Row> actuals, Collection<? extends Row> expecteds) {   
        ValueSource actualSource = actual.value(i);
        ValueSource expectedSource = expected.value(i);
        TInstance actualType = actual.rowType().typeAt(i);
        TInstance expectedType = expected.rowType().typeAt(i);
        if (actualType == null || expectedType == null) {
            assert actualSource.isNull() && expectedSource.isNull();
            return;
        }
        assertTrue(expectedType + " != " + actualType, expectedType.equalsExcludingNullable(actualType));

        
        if(!ValueSources.areEqual(actualSource, expectedSource, expectedType) &&
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
                rows.add(row);
            }
            return rows;
        } finally {
            cursor.close();
        }
    }

    public static Schema schema() {
        return new Schema(new com.foundationdb.ais.model.AkibanInformationSchema());
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
                                     boolean openAllSubCursors)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <HKEY extends HKey> HKEY newHKey(com.foundationdb.ais.model.HKey hKeyMetadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Store getUnderlyingStore() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeRow(Row newRow, TableIndex[] indexes, Collection<GroupIndex> groupIndexes)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRow(Row oldRow, boolean cascadeDefault)
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
        public long sequenceNextValue(Sequence sequence) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long sequenceCurrentValue(Sequence sequence) {
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
        public Key createKey() {
            throw new UnsupportedOperationException();
        }
    }
}
