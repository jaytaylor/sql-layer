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

package com.foundationdb.server.test.it.qp;


import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.CompoundRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.collation.AkCollator;
import org.junit.Test;

import java.util.*;

import static com.foundationdb.qp.operator.API.*;


public class HashJoinIT extends OperatorITBase {

    AkCollator nameCollator;

    @Override
    protected void setupPostCreateSchema() {
        super.setupPostCreateSchema();

        NewRow[] db = new NewRow[]{
                createNewRow(customer, 1L, "northbridge"), // two orders, two addresses
                createNewRow(customer, 2L, "foundation"), // two orders, one address
                createNewRow(customer, 3L, "matrix"), // one order, two addresses
                createNewRow(customer, 4L, "atlas"), // two orders, no addresses
                createNewRow(customer, 5L, "highland"), // no orders, two addresses
                createNewRow(customer, 6L, "flybridge"), // no orders or addresses

                createNewRow(address, 5000L, 5L, "555 5000 st"),
                createNewRow(address, 5001L, 5L, "555 5001 st"),
                createNewRow(address, 1000L, 1L, "111 1000 st"),
                createNewRow(address, 1001L, 1L, "111 1001 st"),
                createNewRow(address, 3000L, 3L, "333 3000 st"),
                createNewRow(address, 3001L, 3L, "333 3001 st"),
                createNewRow(address, 2000L, 2L, "222 2000 st"),

                createNewRow(order, 300L, 3L, "tom"),
                createNewRow(order, 400L, 4L, "jack"),
                createNewRow(order, 401L, 4L, "jack"),
                createNewRow(order, 200L, 2L, "david"),
                createNewRow(order, 201L, 2L, "david"),
                createNewRow(order, 100L, 1L, "ori"),
                createNewRow(order, 101L, 1L, "ori"),
        };
        use(db);
        ciCollator = customerRowType.table().getColumn(1).getCollator();
        nameCollator = orderRowType.table().getColumn(2).getCollator();
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testLeftInputNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(null, groupScan_Default(coi), columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightInputNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(groupScan_Default(coi), null, columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBothInputsNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(null, null, columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testnullFieldsToCompare() {
        int columnsToJoinOn[] = {1};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), columnsToJoinOn, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedFieldsToCompare() {
        int columnsToJoinOn1[] = {1};
        int columnsToJoinOn2[] = {1, 2};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), columnsToJoinOn1, columnsToJoinOn2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyFieldsToCompare() {
        int columnsToJoinOn[] = {};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), columnsToJoinOn, columnsToJoinOn);
    }

    private Operator hashJoinPlan(TableRowType t1, TableRowType t2, int leftJoinFields[], int rightJoinFields[], List<AkCollator> collators) {
        Operator plan =
                hashJoin(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(t1)),
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(t2)),

                        collators,
                        leftJoinFields,
                        rightJoinFields);
        return plan;
    }

    public class JoinedRowType extends CompoundRowType {

        public JoinedRowType(Schema schema, int typeID, RowType first, RowType second) {
            super(schema, typeID, first, second);
        }
    }

    @Test
    public void testSingleColumnJoin() {
        // customer order inner join, done as a general join
        int orderFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        List<AkCollator> collators = Arrays.asList(ciCollator);

        Operator plan = hashJoinPlan(orderRowType, customerRowType,  orderFieldsToCompare,customerFieldsToCompare, collators);
        RowType projectRowType = new JoinedRowType(orderRowType.schema(), 1, orderRowType, customerRowType);
        Row[] expected = new Row[]{
                row(projectRowType, 100L, 1L, "ori", 1L, "northbridge"),
                row(projectRowType, 101L, 1L, "ori", 1L, "northbridge"),
                row(projectRowType, 200L, 2L, "david", 2L, "foundation"),
                row(projectRowType, 201L, 2L, "david", 2L, "foundation"),
                row(projectRowType, 300L, 3L, "tom", 3L, "matrix"),
                row(projectRowType, 400L, 4L, "jack", 4L, "atlas"),
                row(projectRowType, 401L, 4L, "jack", 4L, "atlas"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMultiColumnNestedJoin() {
        int orderFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        List<AkCollator> firstCollators = Arrays.asList(ciCollator);
        Operator firstPlan = hashJoinPlan( orderRowType,customerRowType,  orderFieldsToCompare,customerFieldsToCompare, firstCollators);
        RowType firstProjectRowType = new JoinedRowType(orderRowType.schema(), 1, orderRowType, customerRowType);

        int secondHashFieldsToCompare[] = {1,2};
        List<AkCollator> secondCollators = Arrays.asList(ciCollator, nameCollator);
        Operator secondPlan = hashJoin(firstPlan,
                                       filter_Default(
                                               groupScan_Default(coi),
                                               Collections.singleton(orderRowType)),
                                       secondCollators,
                                       secondHashFieldsToCompare,
                                       secondHashFieldsToCompare);
        RowType secondProjectRowType = new JoinedRowType(orderRowType.schema(), 1, firstProjectRowType, orderRowType);
        Row[] expected = new Row[]{
                row(secondProjectRowType, 100L, 1L, "ori", 1L, "northbridge", 100L, 1L, "ori"),
                row(secondProjectRowType, 100L, 1L, "ori", 1L, "northbridge", 101L, 1L, "ori"),
                row(secondProjectRowType, 101L, 1L, "ori", 1L, "northbridge", 100L, 1L, "ori"),
                row(secondProjectRowType, 101L, 1L, "ori", 1L, "northbridge", 101L, 1L, "ori"),
                row(secondProjectRowType, 200L, 2L, "david", 2L, "foundation", 200L, 2L, "david"),
                row(secondProjectRowType, 200L, 2L, "david", 2L, "foundation", 201L, 2L, "david"),
                row(secondProjectRowType, 201L, 2L, "david", 2L, "foundation", 200L, 2L, "david"),
                row(secondProjectRowType, 201L, 2L, "david", 2L, "foundation", 201L, 2L, "david"),
                row(secondProjectRowType, 300L, 3L, "tom", 3L, "matrix", 300L, 3L, "tom"),
                row(secondProjectRowType, 400L, 4L, "jack", 4L, "atlas", 400L, 4L, "jack"),
                row(secondProjectRowType, 400L, 4L, "jack", 4L, "atlas", 401L, 4L, "jack"),
                row(secondProjectRowType, 401L, 4L, "jack", 4L, "atlas", 400L, 4L, "jack"),
                row(secondProjectRowType, 401L, 4L, "jack", 4L, "atlas", 401L, 4L, "jack"),
        };
        compareRows(expected, cursor(secondPlan, queryContext, queryBindings));
    }
}

