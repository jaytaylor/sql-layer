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

package com.akiban.qp.operator;

import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.store.Store;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.Strings;
import com.akiban.util.tap.InOutTap;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
        
        if(!PValueSources.areEqual(actualSource, expectedSource)) {
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
        Cursor result = plan.cursor(new SimpleQueryContext(ADAPTER));
        reopen(result);
        return result;
    }

    public static void reopen(Cursor cursor) {
        cursor.open();
    }

    public static List<Row> execute(Operator plan) {
        List<Row> rows = new ArrayList<Row>();
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
        public GroupCursor newGroupCursor(GroupTable groupTable)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor newIndexCursor(QueryContext context,
                                     Index index,
                                     IndexKeyRange keyRange,
                                     API.Ordering ordering,
                                     IndexScanSelector selector,
                                     boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <HKEY extends HKey> HKEY newHKey(com.akiban.ais.model.HKey hKeyMetadata)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Store getUnderlyingStore() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateRow(Row oldRow, Row newRow, boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void writeRow(Row newRow, boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteRow(Row oldRow, boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterRow(Row oldRow, Row newRow, Index[] indexesToMaintain, boolean hKeyChanged, boolean usePValues) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Cursor sort(QueryContext context,
                           Cursor input,
                           RowType rowType,
                           API.Ordering ordering,
                           API.SortOption sortOption,
                           InOutTap loadTap,
                           boolean usePValues)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getQueryTimeoutSec()
        {
            return -1;
        }

        @Override
        public long rowCount(RowType tableType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long hash(ValueSource valueSource, AkCollator collator)
        {
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
    }
}
