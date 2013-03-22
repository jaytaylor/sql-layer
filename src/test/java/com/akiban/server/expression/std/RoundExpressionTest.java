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

package com.akiban.server.expression.std;

import java.math.BigInteger;
import java.math.BigDecimal;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;

import static com.akiban.server.expression.std.ExprUtil.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class RoundExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;

    private List<? extends Expression> args;
    private Expression expected;
    
    public RoundExpressionTest(List<? extends Expression> args, Expression exp)
    {
        this.args = args;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
        
        // test with primitive types (double/float, long/int)
        param(p, Arrays.asList(lit(150.12345), lit(2)), lit(150.12));
        param(p, Arrays.asList(lit(123.3f), lit(-1)), lit(120.0f));
        param(p, Arrays.asList(lit(12L), lit(-1L)), lit(10L));
        param(p, Arrays.asList(lit(123.4), lit(-10L)), lit(0.0d));
        param(p, Arrays.asList(lit(16.1234), lit(-1)), lit(20.0));
        param(p, Arrays.asList(lit(5L), lit(3L)), lit(5L));
        param(p, Arrays.asList(lit(10L), lit(-5L)), lit(0L));
        param(p, Arrays.asList(lit(0.49999d), lit(0L)), lit(0.0));
        
        // test with objects (BigDecimal/String/BigInteger)
        
        // test with BigDecimal
        param(p, 
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, new BigDecimal("5.123000")),
                              lit(4L)), 
                new LiteralExpression(AkType.DECIMAL, new BigDecimal("5.1230")));
        
        param(p,
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, new BigDecimal("12345.123")),
                              lit(-2L)),
                new LiteralExpression(AkType.DECIMAL, new BigDecimal("12300")));
        
        param(p,
                Arrays.asList(new LiteralExpression(AkType.DECIMAL, new BigDecimal("123.0")),
                              lit(-10L)),
                new LiteralExpression(AkType.DECIMAL, BigDecimal.ZERO));
        
        // test with BigInteger
        param(p,
              Arrays.asList(new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(16)),
                            lit(-1L)),
              new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(20)));
        
        param(p,
              Arrays.asList(new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(12)),
                            lit(4L)),
              new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(12)));
        
        // tests with string
        param(p, Arrays.asList(lit("123.456")), lit(123.0d));
        param(p, Arrays.asList(lit("456.780"), lit(-2L)), lit(500.0d));
        param(p, Arrays.asList(lit("12"), lit(-5L)), lit(0.0d));
        
        // test with nulls
        param(p, Arrays.asList(LiteralExpression.forNull()), null);
        param(p, Arrays.asList(lit(12.3d), LiteralExpression.forNull()), null);
        param(p, Arrays.asList(LiteralExpression.forNull(), LiteralExpression.forNull()), null);
        
        // test invalid arg type
        param(p, Arrays.asList(new LiteralExpression(AkType.DATE, 231231L)), null);
        param(p, Arrays.asList(new LiteralExpression(AkType.TIME, 123010L), lit(2L)), null);
        
        return p.asList();
    }
    
    private static void param(ParameterizationBuilder p, List<? extends Expression> args, Expression exp)
    {
        p.add("ROUND(" + args + ")" , args, exp);
    }
    
    @Test
    public void test()
    {
        alreadyExc = true;
       
        ValueSource top = new RoundExpression(args).evaluation().eval();        
        if (expected == null)
            assertTrue("Top should be NULL", top.isNull());
        else
            assertEquals(new ValueHolder(expected.evaluation().eval()), 
                         new ValueHolder(top));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.LONG, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return RoundExpression.ROUND;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
