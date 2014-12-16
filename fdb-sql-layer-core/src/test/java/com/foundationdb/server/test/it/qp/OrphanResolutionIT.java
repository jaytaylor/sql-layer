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

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.BindableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;

import org.junit.Test;

import java.util.Arrays;

import static com.foundationdb.qp.operator.API.*;

public class OrphanResolutionIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "px int",
            "primary key(pid)");
        child = createTable(
            "schema", "child",
            "pid int",
            "cx int",
            "grouping foreign key(pid) references parent(pid)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        group = group(parent);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(child, 1L, 100L),
            row(child, 1L, 101L),
        };
        use(db);
    }

    // Inspired by bug 1020342.

    @Test
    public void test()
    {
        Operator insertPlan = insert_Returning(valuesScan_Default(Arrays.asList(parentRow(1, 10)), parentRowType));
        runPlan(queryContext, queryBindings, insertPlan);
        // Execution of insertPlan used to hang before 1020342 was fixed.
        Row[] expected = new Row[] {
            row(parentRowType, 1L, 10L),
            // Last column of child rows is generated PK value
            row(childRowType, 1L, 100L, 1L),
            row(childRowType, 1L, 101L, 2L),
        };
        compareRows(expected, cursor(groupScan_Default(group), queryContext, queryBindings));
    }

    private BindableRow parentRow(int pid, int px)
    {
        return BindableRow.of(parentRowType, Arrays.asList(ExpressionGenerators.literal(pid, MNumeric.INT.instance(true)),
                                                           ExpressionGenerators.literal(px, MNumeric.INT.instance(true))));
    }

    private int parent;
    private int child;
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private Group group;
}
