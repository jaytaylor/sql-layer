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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import org.junit.Before;
import org.junit.Test;


import static com.akiban.qp.operator.API.*;

public final class Sort_MixedColumnTypesIT extends ITBase {
    @Before
    public void createSchema() {
        customer = createTable(
                "schema", "customer",
                "cid int not null primary key",
                "name varchar(32)",
                "importance decimal(5,2)"
        );
        createIndex(
                "schema", "customer", "importance_and_name",
                "importance", "name"
        );
        // These values have been picked for the following criteria:
        // - all three columns (pk and the two indexed columns) are of different types
        // - neither 'name' nor 'importance' are consistently ordered relative to cid
        // - when the rows are ordered by name, they are unordered by importance
        // - when the rows are ordered by importance, they are unordered by name
        writeRows(
                createNewRow(customer, 1L, "Ccc", "100.00"),
                createNewRow(customer, 2L, "Aaa", "75.25"),
                createNewRow(customer, 3L, "Bbb", "120.00"),
                createNewRow(customer, 4L, "Aaa", "32.00")
        );

        schema = new Schema(ddl().getAIS(session()));
        UserTable cTable = getUserTable(customer);
        customerRowType = schema.userTableRowType(cTable);
        customerGroup = cTable.getGroup();

    }

    @Test
    public void unidirectional() {
        Ordering ordering = API.ordering();
        orderBy(ordering, 1, true);
        orderBy(ordering, 2, true);

        Operator plan = sort_Tree(
                groupScan_Default(customerGroup),
                customerRowType,
                ordering,
                SortOption.PRESERVE_DUPLICATES
        );
        Row[] expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.DECIMAL)
                .row(4L, "Aaa", "32.00")
                .row(2L, "Aaa", "75.25")
                .row(3L, "Bbb", "120.00")
                .row(1L, "Ccc", "100.00")
                .rows().toArray(new Row[4]);
        compareRows(expected, cursor(plan));
    }

    @Test
    public void mixed() {
        Ordering ordering = API.ordering();
        orderBy(ordering, 1, true);
        orderBy(ordering, 2, false);

        Operator plan = sort_Tree(
                groupScan_Default(customerGroup),
                customerRowType,
                ordering,
                SortOption.PRESERVE_DUPLICATES
        );
        Row[] expected = new RowsBuilder(AkType.LONG, AkType.VARCHAR, AkType.DECIMAL)
                .row(2L, "Aaa", "75.25")
                .row(4L, "Aaa", "32.00")
                .row(3L, "Bbb", "120.00")
                .row(1L, "Ccc", "100.00")
                .rows().toArray(new Row[4]);
        compareRows(expected, cursor(plan));
    }

    private Cursor cursor(Operator plan) {
        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        QueryContext context = new SimpleQueryContext(adapter);
        return API.cursor(plan, context);
    }

    private void orderBy(Ordering ordering, int fieldPos, boolean ascending) {
        Expression oFieldExpression;
        TPreparedExpression tFieldExpression;
        if (Types3Switch.ON) {
            tFieldExpression = new TPreparedField(customerRowType.typeInstanceAt(fieldPos), fieldPos);
            oFieldExpression = null;
        }
        else {
            tFieldExpression = null;
            oFieldExpression = new FieldExpression(customerRowType, fieldPos);
        }
        ordering.append(oFieldExpression, tFieldExpression, ascending);
    }

    private Schema schema;
    private int customer;
    private Group customerGroup;
    private UserTableRowType customerRowType;
}
