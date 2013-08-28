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

import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.qp.exec.UpdateResult;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.RowBase;
import static com.foundationdb.qp.operator.API.*;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

public class UpdateIT extends OperatorITBase
{
    @Test
    public void basicUpdate() throws Exception {
        use(db);

        UpdateFunction updateFunction = new UpdateFunction() {

            @Override
            public boolean rowIsSelected(Row row) {
                return row.rowType().equals(customerRowType);
            }

            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                String name = original.pvalue(1).getString();
                // TODO eventually use Expression for this
                name = name.toUpperCase();
                name = name + name;
                return new OverlayingRow(original).overlay(1, name);
            }
        };

        Operator groupScan = groupScan_Default(coi);
        UpdatePlannable updateOperator = update_Default(groupScan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext, queryBindings);
        assertEquals("rows modified", 2, result.rowsModified());
        assertEquals("rows touched", db.length, result.rowsTouched());

        Cursor executable = cursor(groupScan, queryContext, queryBindings);
        RowBase[] expected = new RowBase[]{row(customerRowType, 1L, "XYZXYZ"),
                                           row(orderRowType, 11L, 1L, "ori"),
                                           row(itemRowType, 111L, 11L),
                                           row(itemRowType, 112L, 11L),
                                           row(orderRowType, 12L, 1L, "david"),
                                           row(itemRowType, 121L, 12L),
                                           row(itemRowType, 122L, 12L),
                                           row(customerRowType, 2L, "ABCABC"),
                                           row(orderRowType, 21L, 2L, "tom"),
                                           row(itemRowType, 211L, 21L),
                                           row(itemRowType, 212L, 21L),
                                           row(orderRowType, 22L, 2L, "jack"),
                                           row(itemRowType, 221L, 22L),
                                           row(itemRowType, 222L, 22L)
        };
        compareRows(expected, executable);
    }
    
    @Test
    public void changePrimaryKeys() throws Exception {
        use(db);

        Operator scan = filter_Default(
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType),
                coi,
                itemIidIndexRowType,
                Arrays.asList(itemRowType),
                InputPreservationOption.DISCARD_INPUT),
            Arrays.asList(itemRowType));
        
        UpdateFunction updateFunction = new UpdateFunction() {
                @Override
                public boolean rowIsSelected(Row row) {
                    return row.rowType().equals(itemRowType);
                }

                @Override
                public Row evaluate(Row original, QueryContext context, QueryBindings bindings) { 
                    long id = original.pvalue(0).getInt64();
                    // Make smaller to avoid Halloween (see next test).
                    return new OverlayingRow(original).overlay(0, id - 100);
                }
            };

        UpdatePlannable updateOperator = update_Default(scan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext, queryBindings);
        assertEquals("rows touched", 8, result.rowsTouched());
        assertEquals("rows modified", 8, result.rowsModified());

        Cursor executable = cursor(scan, queryContext, queryBindings);
        RowBase[] expected = new RowBase[] { 
            row(itemRowType, 11L, 11L),
            row(itemRowType, 12L, 11L),
            row(itemRowType, 21L, 12L),
            row(itemRowType, 22L, 12L),
            row(itemRowType, 111L, 21L),
            row(itemRowType, 112L, 21L),
            row(itemRowType, 121L, 22L),
            row(itemRowType, 122L, 22L),
        };
        compareRows(expected, executable);
    }

    @Ignore("Halloween support")
    @Test
    // http://en.wikipedia.org/wiki/Halloween_Problem
    public void halloweenProblem() throws Exception {
        use(db);
        adapter.enterUpdateStep(true); // Enter isolation mode.

        Operator scan = filter_Default(
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType),
                coi,
                itemIidIndexRowType,
                Arrays.asList(itemRowType),
                InputPreservationOption.DISCARD_INPUT),
            Arrays.asList(itemRowType));
        
        UpdateFunction updateFunction = new UpdateFunction() {

                @Override
                public boolean rowIsSelected(Row row) {
                    return row.rowType().equals(itemRowType);
                }

                @Override
                public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                    long id = original.pvalue(0).getInt64();
                    return new OverlayingRow(original).overlay(0, 1000 + id);
                }
            };

        UpdatePlannable updateOperator = update_Default(scan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext, queryBindings);
        assertEquals("rows touched", 8, result.rowsTouched());
        assertEquals("rows modified", 8, result.rowsModified());

        adapter.enterUpdateStep(); // Make changes visible.

        Cursor executable = cursor(scan, queryContext, queryBindings);
        RowBase[] expected = new RowBase[] { 
            row(itemRowType, 1111L, 11L),
            row(itemRowType, 1112L, 11L),
            row(itemRowType, 1121L, 12L),
            row(itemRowType, 1122L, 12L),
            row(itemRowType, 1211L, 21L),
            row(itemRowType, 1212L, 21L),
            row(itemRowType, 1221L, 22L),
            row(itemRowType, 1222L, 22L),
        };
        compareRows(expected, executable);
    }


    @Test
    public void updateCustomer() {
        use(db);
        doUpdate();
        compareRows(
                array(TestRow.class,
                      row(customerRowType, 1L, "xyz"),
                      row(customerRowType, 2L, "zzz")
                      ),
                cursor(
                        filter_Default(
                                groupScan_Default(coi),
                                Collections.singleton(customerRowType)),
                        queryContext, queryBindings
                )
        );
    }

    @Test
    public void updateCustomerCheckNameIndex() {
        use(db);
        doUpdate();
        compareRows(
                array(RowBase.class,
                      row(customerNameIndexRowType, "xyz", 1L),
                      row(customerNameIndexRowType, "zzz", 2L)
                      ),
                cursor(
                        indexScan_Default(
                                customerNameIndexRowType,
                                IndexKeyRange.unbounded(customerNameIndexRowType),
                                new API.Ordering()),
                        queryContext, queryBindings
                ));
    }

    @Test
    public void updateCustomerCheckNameItemOidGroupIndex() {
        use(db);
        doUpdate();
        compareRows(
                array(RowBase.class,
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 111L),
                      row(customerNameItemOidIndexRowType, "xyz", 11L, 1L, 11L, 112L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 121L),
                      row(customerNameItemOidIndexRowType, "xyz", 12L, 1L, 12L, 122L),
                      row(customerNameItemOidIndexRowType, "zzz", 21L, 2L, 21L, 211L),
                      row(customerNameItemOidIndexRowType, "zzz", 21L, 2L, 21L, 212L),
                      row(customerNameItemOidIndexRowType, "zzz", 22L, 2L, 22L, 221L),
                      row(customerNameItemOidIndexRowType, "zzz", 22L, 2L, 22L, 222L)
                ),
                cursor(
                        indexScan_Default(
                                customerNameItemOidIndexRowType,
                                IndexKeyRange.unbounded(customerNameItemOidIndexRowType),
                                new API.Ordering(),
                                customerRowType),
                        queryContext, queryBindings
                ));
    }

    private void doUpdate() {
        Row[] rows = {
                row(customerRowType, new Object[]{2, "abc"})
        };
        UpdateFunction updateFunction = new UpdateFunction() {
            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                return row(customerRowType, 2L, "zzz");
            }

            @Override
            public boolean rowIsSelected(Row row) {
                return true;
            }
        };
        UpdatePlannable insertPlan = update_Default(rowsToValueScan(rows), updateFunction);
        UpdateResult result = insertPlan.run(queryContext, queryBindings);
        assertEquals("rows touched", rows.length, result.rowsTouched());
        assertEquals("rows modified", rows.length, result.rowsModified());
    }
}
