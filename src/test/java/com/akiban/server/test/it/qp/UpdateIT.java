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
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
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

import org.junit.Test;
import static org.junit.Assert.assertEquals;

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
            public Row evaluate(Row original, Bindings bindings) {
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
        UpdateResult result = updateOperator.run(NO_BINDINGS, adapter);
        assertEquals("rows modified", 2, result.rowsModified());
        assertEquals("rows touched", db.length, result.rowsTouched());

        Cursor executable = cursor(groupScan, adapter);
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
    public void halloween() throws Exception {
        // A table without any primary key will have a generated one,
        // which changes on update.
        final int tableId = createTable("schema", "nokey", "i int, j int");
        schema = new Schema(rowDefCache().ais());
        adapter = persistitAdapter(schema);
        final RowType tableRowType = schema.userTableRowType(userTable(tableId));

        NewRow[] db = new NewRow[] { createNewRow(tableId, 1L, 0L) };
        use(db);

        UpdateFunction updateFunction = new UpdateFunction() {
                @Override
                public boolean rowIsSelected(Row row) {
                    return row.rowType().equals(tableRowType);
                }

                @Override
                public Row evaluate(Row original, Bindings bindings) {
                    long j = original.eval(1).getInt();
                    return new OverlayingRow(original).overlay(1, j+1);
                }
            };

        Operator groupScan = groupScan_Default(groupTable(tableId));
        UpdatePlannable updateOperator = update_Default(groupScan, updateFunction);
        UpdateResult result = updateOperator.run(NO_BINDINGS, adapter);
        assertEquals("rows touched", 1, result.rowsTouched());
        assertEquals("rows modified", 1, result.rowsModified());

        Cursor executable = cursor(groupScan, adapter);
        RowBase[] expected = new RowBase[] { row(tableRowType, 1L, 1L) };
        compareRows(expected, executable);
    }

}
