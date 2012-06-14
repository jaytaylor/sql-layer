/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
