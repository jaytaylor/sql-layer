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
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import org.junit.Test;

import java.util.*;

import static com.foundationdb.qp.operator.API.*;

public class HashTableLookup_DefaultIT extends OperatorITBase {

    static int ROW_BINDING_POSITION = 100;
    static int TABLE_BINDING_POSITION = 200;
    private int fullAddress;
    TableRowType fullAddressRowType;
    private RowType projectRowType;
    List<TPreparedExpression> genericExpressionList;
    List<TPreparedExpression> emptyExpressionList;

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
        genericExpressionList = new ArrayList<>();
        genericExpressionList.add(new TPreparedField(customerRowType.typeAt(0), 0));
        emptyExpressionList = new ArrayList<>();
    }

    /** Test argument HashJoinLookup_Default */

    @Test(expected = IllegalArgumentException.class)
    public void testHashJoinbindingsSame() {
        hashTableLookup_Default(null, genericExpressionList, false, ROW_BINDING_POSITION, ROW_BINDING_POSITION, customerRowType, customerRowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHashJoinEmptyComparisonFields() {
        hashTableLookup_Default(null, emptyExpressionList, false, ROW_BINDING_POSITION, TABLE_BINDING_POSITION, customerRowType, customerRowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHashJoinNullComparisonFields() {
        hashTableLookup_Default(null, null, false, ROW_BINDING_POSITION, TABLE_BINDING_POSITION, customerRowType, customerRowType);
    }

    /** Test arguments using_HashTable  */

    @Test(expected = IllegalArgumentException.class)
    public void testUsingHashJoinRightInputNull() {
        using_HashTable(groupScan_Default(coi), customerRowType, genericExpressionList, TABLE_BINDING_POSITION, null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUsingHashJoinLeftInputNull() {
        using_HashTable(null, customerRowType, genericExpressionList, TABLE_BINDING_POSITION, groupScan_Default(coi), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUsingHashJoinBothInputsNull() {
        using_HashTable(null, customerRowType,genericExpressionList,  TABLE_BINDING_POSITION, null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUsingHashJoinEmptyComparisonFields() {
        using_HashTable(groupScan_Default(coi), customerRowType, emptyExpressionList, TABLE_BINDING_POSITION, groupScan_Default(coi), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUsingHashJoinNullComparisonFields() {
        using_HashTable(groupScan_Default(coi), customerRowType, null, TABLE_BINDING_POSITION, groupScan_Default(coi), null);
    }

    /** Hash join tests **/

    @Test
    public void testSingleColumnJoin() {
        int orderFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        Operator plan = hashJoinPlan(orderRowType, customerRowType,  orderFieldsToCompare,customerFieldsToCompare, null, false);
        Row[] expected = new Row[]{
                row(projectRowType, 100L, 1L, "ori","northbridge"),
                row(projectRowType, 101L, 1L, "ori", "northbridge"),
                row(projectRowType, 200L, 2L, "david", "foundation"),
                row(projectRowType, 201L, 2L, "david", "foundation"),
                row(projectRowType, 300L, 3L, "tom", "matrix"),
                row(projectRowType, 400L, 4L, "jack", "atlas"),
                row(projectRowType, 401L, 4L, "jack", "atlas"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testMultiColumnNestedJoin() {
        int orderFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        Operator firstPlan = hashJoinPlan( orderRowType,customerRowType,  orderFieldsToCompare,customerFieldsToCompare, null, false);
        int secondHashFieldsToCompare[] = {1,2};
        List<AkCollator> secondCollators = Arrays.asList(null, ciCollator);
        Operator plan = hashJoinPlan(projectRowType,
                                     orderRowType,
                                     firstPlan,
                                     filter_Default(
                                             groupScan_Default(orderRowType.table().getGroup()),
                                             Collections.singleton(orderRowType)
                                     ),
                                     secondHashFieldsToCompare,
                                     secondHashFieldsToCompare,
                                     secondCollators,
                                     false
                        );
        Row[] expected = new Row[]{
                row(projectRowType, 100L, 1L, "ori", "northbridge", 100L),
                row(projectRowType, 100L, 1L, "ori", "northbridge", 101L),
                row(projectRowType, 101L, 1L, "ori", "northbridge", 100L),
                row(projectRowType, 101L, 1L, "ori", "northbridge", 101L),
                row(projectRowType, 200L, 2L, "david", "foundation", 200L),
                row(projectRowType, 200L, 2L, "david", "foundation", 201L),
                row(projectRowType, 201L, 2L, "david", "foundation", 200L),
                row(projectRowType, 201L, 2L, "david", "foundation", 201L),
                row(projectRowType, 300L, 3L, "tom", "matrix", 300L),
                row(projectRowType, 400L, 4L, "jack", "atlas", 400L),
                row(projectRowType, 400L, 4L, "jack", "atlas", 401L),
                row(projectRowType, 401L, 4L, "jack", "atlas", 400L),
                row(projectRowType, 401L, 4L, "jack", "atlas", 401L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testInnerJoin() {
        int addressFieldsToCompare[] = {1};
        int customerFieldsToCompare[] = {0};
        List<AkCollator> collators = Arrays.asList(ciCollator);
        Operator plan = hashJoinPlan(    addressRowType,
                                         customerRowType,
                                         addressFieldsToCompare,
                                         customerFieldsToCompare,
                                         collators,
                                         false);
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
        int orderFieldsToCompare[] = {0,1,2};
        List<AkCollator> collators = Arrays.asList(null,null,ciCollator);
        Operator plan = hashJoinPlan(orderRowType, orderRowType,  orderFieldsToCompare,orderFieldsToCompare, collators, false);
        Row[] expected = new Row[]{
                row(projectRowType, 100L, 1L, "ori"),
                row(projectRowType, 101L, 1L, "ori"),
                row(projectRowType, 200L, 2L, "david"),
                row(projectRowType, 201L, 2L, "david"),
                row(projectRowType, 300L, 3L, "tom"),
                row(projectRowType, 400L, 4L, "jack"),
                row(projectRowType, 401L, 4L, "jack"),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNoMatch() {
        int orderFieldsToCompare[] = {0};
        Operator plan = hashJoinPlan(orderRowType, customerRowType,  orderFieldsToCompare,orderFieldsToCompare, null, false);
        Row[] expected = new Row[]{
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testLeftOuterJoin() {
        int customerFieldsToCompare[] = {0};
        int orderFieldsToCompare[] = {1};
        Operator plan = hashJoinPlan(customerRowType, orderRowType,  customerFieldsToCompare,orderFieldsToCompare, null, true);
        Row[] expected = new Row[]{
                row(projectRowType, 1L, "northbridge", 100L, "ori"),
                row(projectRowType, 1L, "northbridge", 101L, "ori"),
                row(projectRowType, 2L, "foundation",200L, "david"),
                row(projectRowType, 2L, "foundation",201L, "david"),
                row(projectRowType, 3L, "matrix", 300L, "tom"),
                row(projectRowType, 4L, "atlas",  400L, "jack"),
                row(projectRowType, 4L, "atlas",  401L, "jack"),
                row(customerRowType, 5L, "highland"),
                row(customerRowType, 6L, "flybridge")
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    @Test
    public void testNullColumns() {
        int FieldsToCompare[] = {1};
        Operator plan = hashJoinPlan(itemRowType, itemRowType,  FieldsToCompare,FieldsToCompare, null, false);
        Row[] expected = new Row[]{
                row(projectRowType, 111L, null, 111L),
                row(projectRowType, 111L, null, 112L),
                row(projectRowType, 111L, null, 211L),
                row(projectRowType, 111L, null, 222L),
                row(projectRowType, 112L, null, 111L),
                row(projectRowType, 112L, null, 112L),
                row(projectRowType, 112L, null, 211L),
                row(projectRowType, 112L, null, 222L),
                row(projectRowType, 211L, null, 111L),
                row(projectRowType, 211L, null, 112L),
                row(projectRowType, 211L, null, 211L),
                row(projectRowType, 211L, null, 222L),
                row(projectRowType, 222L, null, 111L),
                row(projectRowType, 222L, null, 112L),
                row(projectRowType, 222L, null, 211L),
                row(projectRowType, 222L, null, 222L),
                row(projectRowType, 121L, 12L, 121L),
                row(projectRowType, 121L, 12L, 122L),
                row(projectRowType, 122L, 12L, 121L),
                row(projectRowType, 122L, 12L, 122L),
                row(projectRowType, 212L, 21L, 212L),
                row(projectRowType, 221L, 22L, 221L)
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator hashJoinPlan( RowType outerRowType,
                                   RowType innerRowType,
                                   int outerJoinFields[],
                                   int innerJoinFields[],
                                   List<AkCollator> collators,
                                   boolean leftOuterJoin) {
        return hashJoinPlan(outerRowType,
                     innerRowType,
                     filter_Default(
                            groupScan_Default(outerRowType.table().getGroup()),
                            Collections.singleton(outerRowType)
                     ),
                     filter_Default(
                            groupScan_Default(innerRowType.table().getGroup()),
                            Collections.singleton(innerRowType)),
                     outerJoinFields,
                     innerJoinFields,
                     collators,
                     leftOuterJoin
        );
    }

    private Operator hashJoinPlan( RowType outerRowType,
                                   RowType innerRowType,
                                   Operator outerStream,
                                   Operator innerStream,
                                   int outerJoinFields[],
                                   int innerJoinFields[],
                                   List<AkCollator> collators,
                                   boolean leftOuterJoin) {

        List<TPreparedExpression> expressions = new ArrayList<>();
        for( int i = 0; i < outerRowType.nFields(); i++){
            expressions.add(new TPreparedBoundField(outerRowType, ROW_BINDING_POSITION, i));
        }
        for( int i = 0, j = 0; i < innerRowType.nFields(); i++){
            if(j < innerJoinFields.length && innerJoinFields[j] == i) {
                j++;
            }else{
                expressions.add(new TPreparedField(innerRowType.typeAt(i), i));
            }
        }

        List<TPreparedExpression> outerJoinExpressions = new ArrayList<>();
        for(int i : outerJoinFields){
            outerJoinExpressions.add(new TPreparedField(outerRowType.typeAt(i), i));
        }
        List<TPreparedExpression> innerJoinExpressions = new ArrayList<>();
        for(int i : innerJoinFields){
            innerJoinExpressions.add(new TPreparedField(innerRowType.typeAt(i), i));
        }

        Operator project = project_Default(
                hashTableLookup_Default(
                        collators,
                        outerJoinExpressions,
                        leftOuterJoin,
                        TABLE_BINDING_POSITION,
                        ROW_BINDING_POSITION,
                        outerRowType,
                        innerRowType
                ),
                innerRowType,
                expressions
        );

        projectRowType = project.rowType();

        return using_HashTable(
                innerStream,
                innerRowType,
                innerJoinExpressions,
                TABLE_BINDING_POSITION++,
                map_NestedLoops(
                        outerStream,
                        project,
                        ROW_BINDING_POSITION++,
                        false,
                        1
                ),
                collators
        );
    }
}

