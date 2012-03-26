/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.server.expression.std;

import com.akiban.junit.Parameterization;
import com.akiban.server.expression.ExpressionComposer;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class ExportSetExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private ValueHolder expected;
    private List<? extends Expression> args;
    private boolean expectException;
    
    public ExportSetExpressionTest(List<? extends Expression> args, ValueHolder expected, boolean error)
    {
        this.args = args;
        this.expected = expected;
        expectException = error;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        // test 5 args
        param(pb, false, "0,0,0,0,0,0,1,0,0,0", 
                bigInt("64"), lit("1"), lit("0"), lit(","),lit(10L));
        
        param(pb, false, "0000001000", 
                bigInt("64"), lit("1"), lit("0"), lit(""),lit(10L));
        
        param(pb, false, "OFF-OFF-OFF-OFF-OFF-OFF-ON-OFF-OFF-OFF", 
                bigInt("64"), lit("ON"), lit("OFF"), lit("-"), lit(10L));
        
        param(pb, false, "ON-ON-ON-ON-ON-ON-OFF-ON-ON-ON", 
                bigInt("64"), lit("OFF"), lit("ON"), lit("-"), lit(10L));
      
        // test 4 args
        param(pb, false, "N-N-N-N-N-Y-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N-N",
                bigInt("32"), lit("Y"), lit("N"), lit("-"));
        param(pb, false, "-_-_--__________________________________________________________",
                bigInt("53"), lit("-"), lit("_"), lit(""));
        
        // test 3 args
        param(pb, false, "A,A,B,A,A,A,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B,B",
                bigInt("59"), lit("A"), lit("B"));
        param(pb, false, "0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0",
                bigInt("4"), lit("1"), lit("0"));
        param(pb, false, "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1",
                new LiteralExpression(AkType.U_BIGINT, BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE)), lit("1"), lit("0"));
        
        // test negative value
        param(pb, false, "1,1,1,1,1,1,1,1,1,1", 
                bigInt("-1"), lit("1"), lit("0"), lit(","), lit(10L));
       
        // test upper limit
        param(pb, false, "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1",
                bigInt("ffffffffffffffff", 16), lit("1"), lit("0"));
        param(pb, false, "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0", 
                bigInt("0"), lit("1"), lit("0"), lit(","), lit(67));
        
        // test nulls
        param(pb, false, null, lit(null), lit("1"), lit("0"), lit(","), lit(10L));
        param(pb, false, null, bigInt("2"), lit(null), lit("0"));
        
        // test arity
        param(pb, true, null, lit(null));
        param(pb, true, null, lit(null), lit(null), lit(null), lit(null), lit(null), lit(null));
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, boolean error, String expected, Expression ... children)
    {
       pb.add("EXPORT_SET(" + children + ")" + expected, 
               Arrays.asList(children), 
               expected == null ? null : new ValueHolder(AkType.VARCHAR, expected),
               error);
    }
    
    @OnlyIfNot("expectError()")
    @Test
    public void test()
    {
        Expression top = ExportSetExpression.COMPOSER.compose(args);
        if (expected == null)
            assertTrue ("Should be NULL", top.evaluation().eval().isNull());
        else
            assertEquals("EXPORT_SET(" + args + ") ",
                expected,
                top.evaluation().eval());
        alreadyExc = true;
    }
    
    @OnlyIf("expectError()")
    @Test(expected=WrongExpressionArityException.class)
    public void testArity()
    {
        ExportSetExpression.COMPOSER.compose(args);
        alreadyExc = true;
    }
    
    public boolean expectError ()
    {
        return expectException;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.NULL, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ExportSetExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
    private static Expression bigInt (String st)
    {
        return bigInt(st, 10);
    }
    private static Expression bigInt (String st, int base)
    {
        return new LiteralExpression(AkType.U_BIGINT, new BigInteger(st, base));
    }
}
