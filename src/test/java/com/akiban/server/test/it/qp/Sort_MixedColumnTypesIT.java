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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.ExpressionGenerator;
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
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import org.junit.Before;
import org.junit.Test;


import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;

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
        Row[] expected = 
            (Types3Switch.ON ?
             new RowsBuilder(MNumeric.INT.instance(false),
                             MString.VARCHAR.instance(32, true),
                             MNumeric.DECIMAL.instance(5,2, true)) :
             new RowsBuilder(AkType.INT, AkType.VARCHAR, AkType.DECIMAL))
                .row(4, "Aaa", "32.00")
                .row(2, "Aaa", "75.25")
                .row(3, "Bbb", "120.00")
                .row(1, "Ccc", "100.00")
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
        Row[] expected = 
            (Types3Switch.ON ?
             new RowsBuilder(MNumeric.INT.instance(false),
                             MString.VARCHAR.instance(32, true),
                             MNumeric.DECIMAL.instance(5,2, true)) :
             new RowsBuilder(AkType.INT, AkType.VARCHAR, AkType.DECIMAL))
                .row(2, "Aaa", "75.25")
                .row(4, "Aaa", "32.00")
                .row(3, "Bbb", "120.00")
                .row(1, "Ccc", "100.00")
                .rows().toArray(new Row[4]);
        compareRows(expected, cursor(plan));
    }

    private Cursor cursor(Operator plan) {
        StoreAdapter adapter = new PersistitAdapter(schema, store(), treeService(), session(), configService());
        QueryContext context = new SimpleQueryContext(adapter);
        return API.cursor(plan, context);
    }

    private void orderBy(Ordering ordering, int fieldPos, boolean ascending) {
        ExpressionGenerator expression = field(customerRowType, fieldPos);
        ordering.append(expression, ascending);
    }

    private Schema schema;
    private int customer;
    private Group customerGroup;
    private UserTableRowType customerRowType;
}
