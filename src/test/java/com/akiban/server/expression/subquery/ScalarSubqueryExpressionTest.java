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

package com.akiban.server.expression.subquery;

import com.akiban.qp.operator.ExpressionGenerator;
import com.akiban.server.error.SubqueryTooManyRowsException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.OldExpressionTestBase;
import com.akiban.server.expression.std.AbstractBinaryExpression;
import com.akiban.server.expression.std.AbstractTwoArgExpressionEvaluation;
import com.akiban.server.expression.std.Comparison;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.OperatorTestHelper;
import com.akiban.qp.operator.RowsBuilder;
import com.akiban.qp.operator.TestOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.*;

import com.akiban.server.types.AkType;

import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedFunction;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import org.junit.Test;

import java.util.Arrays;
import java.util.Deque;

public class ScalarSubqueryExpressionTest extends OldExpressionTestBase {

    @Test
    public void testScalar() {
        Schema schema = OperatorTestHelper.schema();
        Operator outer = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
                .row((Long)null)
                                          
        );
        Operator inner = new TestOperator(new RowsBuilder(schema, AkType.LONG)
                .row(1L)
                .row(2L)
        );

        ExpressionGenerator equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.NE,
                                    field(inner.rowType(), 0), castResolver());
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        ExpressionGenerator expression = add(
                boundField(outer.rowType(), 1, 0),
                field(inner.rowType(), 0));
        ExpressionGenerator scalar = scalarSubquery(innerPlan, expression,
                outer.rowType(), inner.rowType(), 1);
        ExpressionGenerator outerN = field(outer.rowType(), 0);
        Operator outerPlan = project_Default(outer, outer.rowType(),
                                             Arrays.asList(outerN, scalar));
        Deque<Row> expected = new RowsBuilder(AkType.LONG, AkType.LONG)
            .row(1, 3)
            .row(2, 3)
            .row((Long)null, (Long)null)
            .rows();
        OperatorTestHelper.check(outerPlan, expected);
    }

    @Test(expected = SubqueryTooManyRowsException.class)
    public void testTooManyRows() {
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

        ExpressionGenerator equals = compare(boundField(outer.rowType(), 1, 0),
                                    Comparison.NE,
                                    field(inner.rowType(), 0), castResolver());
        Operator innerPlan = select_HKeyOrdered(inner, inner.rowType(), equals);
        ExpressionGenerator expression = add(
                boundField(outer.rowType(), 1, 0),
                field(inner.rowType(), 0));
        ExpressionGenerator scalar = scalarSubquery(innerPlan, expression,
                outer.rowType(), inner.rowType(), 1);
        ExpressionGenerator outerN = field(outer.rowType(), 0);
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

    private static ExpressionGenerator add(final ExpressionGenerator arg0, final ExpressionGenerator arg1)
    {
        return new ExpressionGenerator() {
            @Override
            public Expression getExpression() {
                Expression left = arg0.getExpression();
                Expression right = arg1.getExpression();
                return new AbstractBinaryExpression(AkType.LONG, left, right) {
                    @Override
                    protected void describe(StringBuilder sb) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public boolean nullIsContaminating() {
                        return true;
                    }

                    @Override
                    public ExpressionEvaluation evaluation() {
                        return new AbstractTwoArgExpressionEvaluation(childrenEvaluations()) {
                            @Override
                            public ValueSource eval() {
                                valueHolder().expectType(AkType.LONG);
                                valueHolder().putLong(left().getLong() + right().getLong());
                                return valueHolder();
                            }
                        };
                    }

                    @Override
                    public String name() {
                        return "plus";
                    }
                };
            }

            @Override
            public TPreparedExpression getTPreparedExpression() {
                TScalar scalar = new TScalarBase() {
                    @Override
                    protected void buildInputSets(TInputSetBuilder builder) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs,
                                              PValueTarget output)
                    {
                        output.putInt64(inputs.get(0).getInt64() + inputs.get(1).getInt64());
                    }

                    @Override
                    public String displayName() {
                        return "plus";
                    }

                    @Override
                    public TOverloadResult resultType() {
                        throw new UnsupportedOperationException();
                    }
                };
                TValidatedScalar validated = new TValidatedScalar(scalar);
                return new TPreparedFunction(
                        validated,
                        MNumeric.BIGINT.instance(true),
                        Arrays.asList(arg0.getTPreparedExpression(), arg1.getTPreparedExpression()),
                        null);
            }
        };
    }
}
