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

import com.akiban.ais.model.Group;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.akiban.util.tap.TapReport;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static com.akiban.qp.operator.API.*;

public class SortPT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "rand int",
            "filler varchar(20)");
        group = group(t);
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        adapter = persistitAdapter(schema);
        queryContext = queryContext((PersistitAdapter) adapter);
    }

    @Test
    public void profileSort()
    {
        InOutTap tap = Operator.OPERATOR_TAP; // Force loading of class and registration of tap.
        Tap.setEnabled(OPERATOR_TAPS, true);
        ordering = ordering();
        ordering.append(new FieldExpression(tRowType, 0), true);
        plan = sort_Tree(groupScan_Default(group), tRowType, ordering, SortOption.PRESERVE_DUPLICATES);
        populateDB(10000000);
        // Warmup
        profileSort(10000, 0, false);
        profileSort(10000, 0, false);
        profileSort(10000, 1, false);
        profileSort(10000, 1, false);
        // Measure ordered input
        profileSort(1000, 0, true);
        profileSort(2500, 0, true);
        profileSort(5000, 0, true);
        profileSort(10000, 0, true);
        profileSort(25000, 0, true);
        profileSort(50000, 0, true);
        profileSort(100000, 0, true);
        profileSort(250000, 0, true);
        profileSort(500000, 0, true);
        profileSort(1000000, 0, true);
        profileSort(2500000, 0, true);
        profileSort(5000000, 0, true);
        profileSort(10000000, 0, true);
        // Measure unordered input
        profileSort(1000, 1, true);
        profileSort(2500, 1, true);
        profileSort(5000, 1, true);
        profileSort(10000, 1, true);
        profileSort(25000, 1, true);
        profileSort(50000, 1, true);
        profileSort(100000, 1, true);
        profileSort(250000, 1, true);
        profileSort(500000, 1, true);
        profileSort(1000000, 1, true);
        profileSort(2500000, 1, true);
        profileSort(5000000, 1, true);
        profileSort(10000000, 1, true);
    }
    
    public void profileSort(int n, int field, boolean print)
    {
        Tap.reset(OPERATOR_TAPS);
        Ordering ordering = ordering();
        ordering.append(new FieldExpression(tRowType, field), true);
        Operator plan = 
            sort_Tree(
                limit_Default(
                    groupScan_Default(group),
                    n),
                tRowType, 
                ordering, 
                SortOption.PRESERVE_DUPLICATES);
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        while (cursor.next() != null) {
        }
        cursor.close();
        if (print) {
            System.out.println("---------------------------------------------------------------------");
            System.out.println(String.format("Sort %s on field %s", n, field == 0 ? "id" : "rand"));
            TapReport[] reports = Tap.getReport(OPERATOR_TAPS);
            for (TapReport report : reports) {
                System.out.println(report);
            }
        }
    }

    private void populateDB(int n)
    {
        for (int id = 0; id < n; id++) {
            dml().writeRow(session(), createNewRow(t, id, random.nextInt(), FILLER));
        }
    }

    private static final String OPERATOR_TAPS = ".*operator.*";
    private static final String FILLER = "xxxxxxxxxxxxxxxxxxxx";

    private final Random random = new Random();
    private int t;
    private Group group;
    private Schema schema;
    private RowType tRowType;
    private StoreAdapter adapter;
    private Ordering ordering;
    private Operator plan;
}
