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

package com.akiban.server.expression.subquery;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.functions.FunctionsRegistryImpl;
import static com.akiban.server.expression.std.Expressions.*;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorTestHelper;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import static com.akiban.qp.operator.API.*;

import com.akiban.server.types.AkType;

import org.junit.Test;

import java.util.Arrays;
import java.util.Deque;

public class ScalarSubqueryExpressionTest {

    private FunctionsRegistry functionsRegistry = new FunctionsRegistryImpl();

    @Test
    public void testScalar() {
        Schema schema = OperatorTestHelper.schema();
        Operator outer = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
                .row(3L)
                .row((Long)null)
                                          
        );
        Operator inner = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
        );

        Expression equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.NE,
                                    field(inner.rowType(), 0));
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        Expression expression = functionsRegistry
            .composer("plus")
            .compose(Arrays.asList(boundField(outer.rowType(), 1, 0),
                                   field(inner.rowType(), 0)));
        Expression scalar = new ScalarSubqueryExpression(innerPlan, expression,
                                                         outer.rowType(), inner.rowType(), 1);
        Expression outerN = field(outer.rowType(), 0);
        Operator outerPlan = project_Default(outer, outer.rowType(),
                                             Arrays.asList(outerN, scalar));
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.LONG)
            .row(1, 3)
            .row(2, 3)
            .row(3, 4)
            .row((Long)null, (Long)null)
            .rows();
        OperatorTestHelper.check(outerPlan, expected);
    }

}
