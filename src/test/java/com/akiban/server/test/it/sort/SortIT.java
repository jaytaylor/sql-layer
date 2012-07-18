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

package com.akiban.server.test.it.sort;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.sort.Sorter;
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
        ordering.append(new FieldExpression(inputOperator.rowType(), 0), true);
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
