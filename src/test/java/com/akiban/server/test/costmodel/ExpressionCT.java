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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.ArithExpression;
import com.akiban.server.expression.std.ArithOps;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import org.junit.Test;

public class ExpressionCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        run(new SmallExpressionFactory());
        run(new MediumExpressionFactory());
        run(new BigExpressionFactory());
        run(new ReallyBigExpressionFactory());
    }
    
    private void run(ExpressionFactory expressionFactory)
    {
        Expression expression = expressionFactory.expression();
        ExpressionEvaluation evaluation = expression.evaluation();
        String label = expressionFactory.description();
        run(evaluation, WARMUP_RUNS, null);
        run(evaluation, MEASURED_RUNS, label);
    }
    
    private void run(ExpressionEvaluation evaluation, int runs, String label)
    {
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
            evaluation.eval().getInt();
        }
        long end = System.nanoTime();
        if (label != null) {
            double averageUsec = (end - start) / (1000.0 * runs * EVALS_PER_RUN);
            System.out.println(String.format("%s:  %s usec/eval", label, averageUsec));
        }
    }

    private static final int WARMUP_RUNS = 100000;
    private static final int MEASURED_RUNS = 20000;
    private static final int EVALS_PER_RUN = 10; 

    private abstract class ExpressionFactory
    {
        public abstract String description();

        public abstract Expression expression();
    }

    private class SmallExpressionFactory extends ExpressionFactory
    {
        @Override
        public String description()
        {
            return "small";
        }

        @Override
        public Expression expression()
        {
            return new LiteralExpression(AkType.INT, 1);
        }
    }

    private class MediumExpressionFactory extends ExpressionFactory
    {
        @Override
        public String description()
        {
            return "medium";
        }

        @Override
        public Expression expression()
        {
            return
                new ArithExpression(
                    new LiteralExpression(AkType.INT, 1),
                    ArithOps.ADD,
                    new LiteralExpression(AkType.INT, 1));
        }
    }

    private class BigExpressionFactory extends ExpressionFactory
    {
        @Override
        public String description()
        {
            return "big";
        }

        @Override
        public Expression expression()
        {
            return
                new ArithExpression(
                    new ArithExpression(
                        new LiteralExpression(AkType.INT, 1),
                        ArithOps.ADD,
                        new LiteralExpression(AkType.INT, 1)),
                    ArithOps.MULTIPLY,
                        new ArithExpression(
                            new LiteralExpression(AkType.INT, 1),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1)));
        }
    }

    private class ReallyBigExpressionFactory extends ExpressionFactory
    {
        @Override
        public String description()
        {
            return "really big";
        }

        @Override
        public Expression expression()
        {
            return
                new ArithExpression(
                    new ArithExpression(
                        new ArithExpression(
                            new LiteralExpression(AkType.INT, 1),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1)),
                        ArithOps.MULTIPLY,
                        new ArithExpression(
                            new LiteralExpression(AkType.INT, 1),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1))),
                    ArithOps.MINUS,
                    new ArithExpression(
                        new ArithExpression(
                            new LiteralExpression(AkType.INT, 1),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1)),
                        ArithOps.MULTIPLY,
                        new ArithExpression(
                            new LiteralExpression(AkType.INT, 1),
                            ArithOps.ADD,
                            new LiteralExpression(AkType.INT, 1))));
        }
    }
}
