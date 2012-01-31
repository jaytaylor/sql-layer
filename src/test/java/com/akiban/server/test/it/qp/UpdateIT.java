/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.qp;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import static com.akiban.qp.operator.API.*;

import com.persistit.Transaction;

import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

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
            public Row evaluate(Row original, QueryContext context) {
                ToObjectValueTarget target = new ToObjectValueTarget();
                target.expectType(AkType.VARCHAR);
                Object obj = Converters.convert(original.eval(1), target).lastConvertedValue();
                String name = (String) obj; // TODO eventually use Expression for this
                name = name.toUpperCase();
                name = name + name;
                return new OverlayingRow(original).overlay(1, name);
            }
        };

        Operator groupScan = groupScan_Default(coi);
        UpdatePlannable updateOperator = update_Default(groupScan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext);
        assertEquals("rows modified", 2, result.rowsModified());
        assertEquals("rows touched", db.length, result.rowsTouched());

        Cursor executable = cursor(groupScan, queryContext);
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
                LookupOption.DISCARD_INPUT),
            Arrays.asList(itemRowType));
        
        UpdateFunction updateFunction = new UpdateFunction() {
                @Override
                public boolean rowIsSelected(Row row) {
                    return row.rowType().equals(itemRowType);
                }

                @Override
                public Row evaluate(Row original, QueryContext context) {
                    long id = original.eval(0).getInt();
                    // Make smaller to avoid Halloween (see next test).
                    return new OverlayingRow(original).overlay(0, id - 100);
                }
            };

        UpdatePlannable updateOperator = update_Default(scan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext);
        assertEquals("rows touched", 8, result.rowsTouched());
        assertEquals("rows modified", 8, result.rowsModified());

        Cursor executable = cursor(scan, queryContext);
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

    @Test @Ignore
    // http://en.wikipedia.org/wiki/Halloween_Problem
    public void halloweenProblem() throws Exception {
        use(db);
        /*
        Transaction transaction = adapter.transaction();
        transaction.incrementStep(); // Enter isolation mode.
        */

        Operator scan = filter_Default(
            ancestorLookup_Default(
                indexScan_Default(itemIidIndexRowType),
                coi,
                itemIidIndexRowType,
                Arrays.asList(itemRowType),
                LookupOption.DISCARD_INPUT),
            Arrays.asList(itemRowType));
        
        UpdateFunction updateFunction = new UpdateFunction() {
                @Override
                public boolean rowIsSelected(Row row) {
                    return row.rowType().equals(itemRowType);
                }

                @Override
                public Row evaluate(Row original, QueryContext context) {
                    long id = original.eval(0).getInt();
                    return new OverlayingRow(original).overlay(0, 1000 + id);
                }
            };

        UpdatePlannable updateOperator = update_Default(scan, updateFunction);
        UpdateResult result = updateOperator.run(queryContext);
        assertEquals("rows touched", 8, result.rowsTouched());
        assertEquals("rows modified", 8, result.rowsModified());

        /*
        transaction.incrementStep(); // Make changes visible.
        */

        Cursor executable = cursor(scan, queryContext);
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

}
