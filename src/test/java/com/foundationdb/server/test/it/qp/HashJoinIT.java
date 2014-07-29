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
import static com.foundationdb.server.test.ExpressionGenerators.field;


public class HashJoinIT extends OperatorITBase {

    private int fullAddress;
    TableRowType fullAddressRowType;

    @Override
    protected void setupCreateSchema() {
        super.setupCreateSchema();
        fullAddress = createTable(
                "schema", "fullAddress",
                "aid int not null primary key",
                "cid int",
                "address varchar(100)",
                "name varchar(20)");
        createIndex("schema", "fullAddress", "name", "name");

    }

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

                createNewRow(item, 111L, null),
                createNewRow(item, 112L, null),
                createNewRow(item, 121L, 12L),
                createNewRow(item, 122L, 12L),
                createNewRow(item, 211L, null),
                createNewRow(item, 212L, 21L),
                createNewRow(item, 221L, 22L),
                createNewRow(item, 222L, null)
        };
        use(db);
        fullAddressRowType = schema.tableRowType(table(fullAddress));

        ciCollator = customerRowType.table().getColumn(1).getCollator();
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testLeftInputNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(null, groupScan_Default(coi), customerRowType, customerRowType, columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRightInputNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(groupScan_Default(coi), null, customerRowType, customerRowType,columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBothInputsNull() {
        int columnsToJoinOn[] = {1};
        hashJoin(null, null, customerRowType, customerRowType,columnsToJoinOn, columnsToJoinOn);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testnullFieldsToCompare() {
        int columnsToJoinOn[] = {1};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), customerRowType, customerRowType,columnsToJoinOn, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMismatchedFieldsToCompare() {
        int columnsToJoinOn1[] = {1};
        int columnsToJoinOn2[] = {1, 2};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), customerRowType, customerRowType,columnsToJoinOn1, columnsToJoinOn2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyFieldsToCompare() {
        int columnsToJoinOn[] = {};
        hashJoin(groupScan_Default(coi), groupScan_Default(coi), customerRowType, customerRowType,columnsToJoinOn, columnsToJoinOn);
    }

    private Operator hashJoinPlan(TableRowType t1, TableRowType t2, int leftJoinFields[], int rightJoinFields[], List<AkCollator> collators, boolean leftOuterJoin) {
        Operator plan =
                hashJoin(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(t1)),
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(t2)),
                        t1,
                        t2,
                        collators,
                        leftJoinFields,
                        rightJoinFields,
                        leftOuterJoin);
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
        Operator plan = hashJoinPlan(orderRowType, customerRowType,  orderFieldsToCompare,customerFieldsToCompare, null, false);
        RowType projectRowType = plan.rowType();
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
        Operator firstPlan = hashJoinPlan( orderRowType,customerRowType,  orderFieldsToCompare,customerFieldsToCompare, null, false);
        RowType firstProjectRowType = new JoinedRowType(orderRowType.schema(), 1, orderRowType, customerRowType);

        int secondHashFieldsToCompare[] = {1,2};
        List<AkCollator> secondCollators = Arrays.asList(null, ciCollator);
        Operator secondPlan = hashJoin(firstPlan,
                                       filter_Default(
                                               groupScan_Default(coi),
                                               Collections.singleton(orderRowType)),
                                       firstProjectRowType,
                                       orderRowType,
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

    @Test
    public void testInnerJoin()
    {
        int addressFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        List<AkCollator> collators = Arrays.asList(ciCollator);
        Operator joinPlan = hashJoinPlan(addressRowType,
                                         customerRowType,
                                         addressFieldsToCompare,
                                         customerFieldsToCompare,
                                         collators,
                                         false);
        // customer order inner join, done as a general join
        RowType projectedRowType = joinPlan.rowType();
        Operator plan =
                project_DefaultTest(
                        joinPlan,
                        projectedRowType,
                        Arrays.asList(field(projectedRowType, 0),
                                      field(projectedRowType, 1),
                                      field(projectedRowType, 2),
                                      field(projectedRowType, 4))
                        );
        Row[] expected = new Row[]{
                row(fullAddressRowType, 1000L, 1L, "111 1000 st", "northbridge"),
                row(fullAddressRowType, 1001L, 1L, "111 1001 st", "northbridge"),
                row(fullAddressRowType, 2000L, 2L, "222 2000 st", "foundation"),
                row(fullAddressRowType, 3000L, 3L, "333 3000 st", "matrix"),
                row(fullAddressRowType, 3001L, 3L, "333 3001 st", "matrix"),
                row(fullAddressRowType, 5000L, 5L, "555 5000 st", "highland"),
                row(fullAddressRowType, 5001L, 5L, "555 5001 st", "highland"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testAllMatch() {
        // customer order inner join, done as a general join
        int orderFieldsToCompare[] = {0,1,2};
        List<AkCollator> collators = Arrays.asList(null,null,ciCollator);

        Operator plan = hashJoinPlan(orderRowType, orderRowType,  orderFieldsToCompare,orderFieldsToCompare, collators, false);
        RowType projectRowType = plan.rowType();
        Row[] expected = new Row[]{
                row(projectRowType, 100L, 1L, "ori", 100L, 1L, "ori"),
                row(projectRowType, 101L, 1L, "ori", 101L, 1L, "ori"),
                row(projectRowType, 200L, 2L, "david",200L, 2L, "david"),
                row(projectRowType, 201L, 2L, "david", 201L, 2L, "david"),
                row(projectRowType, 300L, 3L, "tom", 300L, 3L, "tom"),
                row(projectRowType, 400L, 4L, "jack", 400L, 4L, "jack"),
                row(projectRowType, 401L, 4L, "jack",  401L, 4L, "jack"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNoMatch() {
        // customer order inner join, done as a general join
        int orderFieldsToCompare[] = {0};
        Operator plan = hashJoinPlan(orderRowType, customerRowType,  orderFieldsToCompare,orderFieldsToCompare, null, false);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    @Test
    public void testLeftOuterJoin() {
        // customer order inner join, done as a general join
        int customerFieldsToCompare[] = {0};
        int orderFieldsToCompare[] = {1};
        Operator plan = hashJoinPlan(customerRowType, orderRowType,  customerFieldsToCompare,orderFieldsToCompare, null, true);
        RowType projectRowType = plan.rowType();
        Row[] expected = new Row[]{
                row(projectRowType, 1L, "northbridge", 100L, 1L, "ori"),
                row(projectRowType, 1L, "northbridge", 101L, 1L, "ori"),
                row(projectRowType, 2L, "foundation",200L, 2L, "david"),
                row(projectRowType, 2L, "foundation",201L, 2L, "david"),
                row(projectRowType, 3L, "matrix", 300L, 3L, "tom"),
                row(projectRowType, 4L, "atlas",  400L, 4L, "jack"),
                row(projectRowType, 4L, "atlas",  401L, 4L, "jack"),
                row(customerRowType, 5L, "highland"),
                row(customerRowType, 6L, "flybridge")
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNullColumns() {
        // customer order inner join, done as a general join
        int FieldsToCompare[] = {1};
        Operator plan = hashJoinPlan(itemRowType, itemRowType,  FieldsToCompare,FieldsToCompare, null, false);
        RowType projectRowType = plan.rowType();
        Row[] expected = new Row[]{
                row(projectRowType, 111L, null, 111L, null),
                row(projectRowType, 111L, null, 112L, null),
                row(projectRowType, 111L, null, 211L, null),
                row(projectRowType, 111L, null, 222L, null),
                row(projectRowType, 112L, null, 111L, null),
                row(projectRowType, 112L, null, 112L, null),
                row(projectRowType, 112L, null, 211L, null),
                row(projectRowType, 112L, null, 222L, null),
                row(projectRowType, 211L, null, 111L, null),
                row(projectRowType, 211L, null, 112L, null),
                row(projectRowType, 211L, null, 211L, null),
                row(projectRowType, 211L, null, 222L, null),
                row(projectRowType, 222L, null, 111L, null),
                row(projectRowType, 222L, null, 112L, null),
                row(projectRowType, 222L, null, 211L, null),
                row(projectRowType, 222L, null, 222L, null),
                row(projectRowType, 121L, 12L, 121L, 12L),
                row(projectRowType, 121L, 12L, 122L, 12L),
                row(projectRowType, 122L, 12L, 121L, 12L),
                row(projectRowType, 122L, 12L, 122L, 12L),
                row(projectRowType, 212L, 21L, 212L, 21L),
                row(projectRowType, 221L, 22L, 221L, 22L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

}

