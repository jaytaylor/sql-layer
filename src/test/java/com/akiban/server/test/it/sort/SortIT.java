
package com.akiban.server.test.it.sort;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.indexcursor.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.persistit.exception.PersistitException;
import org.junit.Test;

import static com.akiban.server.test.ExpressionGenerators.field;

public final class SortIT extends ITBase {
    @Test
    public void firstRowHasNulls() throws PersistitException {
        RowsBuilder rowsBuilder = new RowsBuilder(AkType.VARCHAR)
                .row(NullValueSource.only())
                .row("beta")
                .row("alpha");
        Schema schema = SchemaCache.globalSchema(ddl().getAIS(session()));
        PersistitAdapter adapter = persistitAdapter(schema);
        TestOperator inputOperator = new TestOperator(rowsBuilder);

        QueryContext context = queryContext(adapter);
        Cursor inputCursor = API.cursor(inputOperator, context);
        inputCursor.open();
        API.Ordering ordering = new API.Ordering();
        ordering.append(field(inputOperator.rowType(), 0), true);
        Sorter sorter = new Sorter(context,
                                   inputCursor,
                                   inputOperator.rowType(),
                                   ordering,
                                   API.SortOption.PRESERVE_DUPLICATES,
                                   TEST_TAP,
                                   Types3Switch.ON);
        Cursor sortedCursor = sorter.sort();

        // check expected output
        Row[] expected = new RowsBuilder(AkType.VARCHAR)
                .row(NullValueSource.only())
                .row("alpha")
                .row("beta")
                .rows().toArray(new Row[3]);
        compareRows(expected, sortedCursor);
    }


    private static InOutTap TEST_TAP = Tap.createTimer("test");
}
