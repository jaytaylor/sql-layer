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

package com.akiban.server.test.costmodel;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.ArithExpression;
import com.akiban.server.expression.std.ArithOps;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.test.pt.qp.QPProfilePTBase;
import com.akiban.server.types.AkType;
import com.akiban.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.akiban.qp.operator.API.*;

public class ProjectCT extends CostModelBase
{
    @Before
    public void before() throws InvalidOperationException {
        t = createTable(
                "schema", "t",
                "id int not null",
                "x0 int",
                "x1 int",
                "x2 int",
                "x3 int",
                "x4 int",
                "x5 int",
                "x6 int",
                "x7 int",
                "x8 int",
                "x9 int",
                "primary key(id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        group = groupTable(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Test
    public void profileGroupScan() {
        Tap.setEnabled(".*", false);
        populateDB(ROWS);
        profileGroupScan(new SmallExpressionFactory());
        profileGroupScan(new MediumExpressionFactory());
        profileGroupScan(new BigExpressionFactory());
    }

    public void profileGroupScan(ExpressionFactory expressionFactory) {
        System.out.println("-------------------------------------------------------------------------");
        System.out.println(String.format("%s expressions", expressionFactory.description()));
        run(null, WARMUP_RUNS, 1, expressionFactory);
        run("1", MEASURED_RUNS, 1, expressionFactory);
        run("2", MEASURED_RUNS, 2, expressionFactory);
        run("3", MEASURED_RUNS, 3, expressionFactory);
        run("4", MEASURED_RUNS, 4, expressionFactory);
        run("5", MEASURED_RUNS, 5, expressionFactory);
        run("6", MEASURED_RUNS, 6, expressionFactory);
        run("7", MEASURED_RUNS, 7, expressionFactory);
        run("8", MEASURED_RUNS, 8, expressionFactory);
        run("9", MEASURED_RUNS, 9, expressionFactory);
        run("10", MEASURED_RUNS, 10, expressionFactory);
    }

    private void run(String label, int runs, int fields, ExpressionFactory expressionFactory) {
        List<Expression> projectExpressions = new ArrayList<Expression>();
        for (int f = 0; f < fields; f++) {
            projectExpressions.add(expressionFactory.expression(f));
        }
        Operator plan =
                project_Default(
                        groupScan_Default(group),
                        tRowType,
                        projectExpressions);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Row row;
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            switch (fields) {
                case 1:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                    }
                    break;
                case 2:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                    }
                    break;
                case 3:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                    }
                    break;
                case 4:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                    }
                    break;
                case 5:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                    }
                    break;
                case 6:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                        row.eval(5);
                    }
                    break;
                case 7:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                        row.eval(5);
                        row.eval(6);
                    }
                    break;
                case 8:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                        row.eval(5);
                        row.eval(6);
                        row.eval(7);
                    }
                    break;
                case 9:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                        row.eval(5);
                        row.eval(6);
                        row.eval(7);
                        row.eval(8);
                    }
                    break;
                case 10:
                    while ((row = cursor.next()) != null) {
                        row.eval(0);
                        row.eval(1);
                        row.eval(2);
                        row.eval(3);
                        row.eval(4);
                        row.eval(5);
                        row.eval(6);
                        row.eval(7);
                        row.eval(8);
                        row.eval(9);
                    }
                    break;
            }
            cursor.close();
        }
        long end = System.nanoTime();
        if (label != null) {
            double averageMsec = (double) (end - start) / (1000.0 * runs * ROWS * fields);
            System.out.println(String.format("%s:  %s usec/field", label, averageMsec));
        }
    }

    protected void populateDB(int rows) {
        for (int id = 0; id < rows; id++) {
            int x = random.nextInt();
            dml().writeRow(session(), createNewRow(t, id, x, x, x, x, x, x, x, x, x, x));
        }
    }

    private static final int ROWS = 100000;
    private static final int WARMUP_RUNS = 10;
    private static final int MEASURED_RUNS = 5;

    private final Random random = new Random();
    private int t;
    private RowType tRowType;
    private GroupTable group;

    private abstract class ExpressionFactory {
        public abstract String description();

        public abstract Expression expression(int i);
    }

    private class SmallExpressionFactory extends ExpressionFactory {
        @Override
        public String description() {
            return "small";
        }

        @Override
        public Expression expression(int i) {
            return new FieldExpression(tRowType, i);
        }
    }

    private class MediumExpressionFactory extends ExpressionFactory {
        @Override
        public String description() {
            return "medium";
        }

        @Override
        public Expression expression(int i) {
            return
                    new ArithExpression(
                            new FieldExpression(tRowType, i),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1));
        }
    }

    private class BigExpressionFactory extends ExpressionFactory {
        @Override
        public String description() {
            return "big";
        }

        @Override
        public Expression expression(int i) {
            return
                    new ArithExpression(
                            new ArithExpression(
                                    new FieldExpression(tRowType, i),
                                    ArithOps.ADD,
                                    new LiteralExpression(AkType.INT, 1)),
                            ArithOps.MULTIPLY,
                            new FieldExpression(tRowType, i));
        }
    }
}
