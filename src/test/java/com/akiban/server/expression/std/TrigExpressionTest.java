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

package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.OnlyIf;
import com.akiban.server.error.OverflowException;
import com.akiban.server.expression.std.TrigExpression.TrigName;
import java.util.List;
import org.junit.runner.RunWith;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner;
import org.junit.Test;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import static org.junit.Assert.*;


@RunWith(NamedParameterizedRunner.class)
public class TrigExpressionTest extends ComposedExpressionTestBase
{
    private double input1;
    private double input2;
    private double expected;
    private TrigExpression.TrigName name;
    private boolean expectExc;

    private static boolean alreadyExc = false;
    public TrigExpressionTest (double input, double expected, 
            TrigExpression.TrigName name, double input2, boolean expectExc)
    {
        this.input1 = input;
        this.expected = expected;
        this.name = name;
        this.input2 = input2;
        this.expectExc = expectExc;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
                
        // SIN 
        param (pb, Math.PI / 6, 0.5, TrigName.SIN, 0, false);
        param (pb, Math.PI / 2, 1, TrigName.SIN, 0, false);
        
        // COS
        param (pb, Math.PI / 3, 0.5, TrigName.COS, 0, false);
        param (pb, 0, 1, TrigName.COS, 0, false);
        
        // TAN
        param(pb, Math.PI / 4, 1, TrigName.TAN, 0, false);
        param(pb, Math.PI / 2, Math.tan(Math.PI /2), TrigName.TAN, 0, false); // will NOT throw overflow exception
        
        // COT
        param(pb, Double.POSITIVE_INFINITY, Double.NaN, TrigName.COT, 0, false);
        param(pb, 0.0 , Double.NEGATIVE_INFINITY, TrigName.COT, 0, true); // expect overflow exception
        
        // ASIN
        param(pb, 0.5, Math.PI / 6, TrigName.ASIN, 0, false);
        param(pb, 1, Math.PI / 2, TrigName.ASIN, 0, false);
        
        // ACOS
        param(pb, 0.5, Math.PI / 3, TrigName.ACOS, 0, false);
        param(pb, 1, 0, TrigName.ACOS, 0, false);
        
        // ATAN
        param(pb, 1, Math.PI / 4, TrigName.ATAN, 0, false);
        param(pb, 0, 0, TrigName.ATAN, 0, false);
        
        // ATAN2
        param(pb, 1, Math.PI /2 , TrigName.ATAN2, 0, false);
        param(pb, Double.NaN, Double.NaN, TrigName.ATAN2, 1, false);
        
        // COSH
        param(pb, 0, 1, TrigName.COSH, 0, false);
        param(pb, 1, (Math.E * Math.E + 1)/ 2 / Math.E,TrigName.COSH, 0, false);
        
        // SINH
        param(pb, 0, 0, TrigName.SINH, 0, false);
        param(pb, 1,(Math.E * Math.E - 1)/ 2 / Math.E, TrigName.SINH, 0, false);
        
        // TANH
        param(pb, 0, 0, TrigName.TANH, 0, false);
        param(pb, 1, (Math.E * Math.E - 1) / (Math.E * Math.E + 1), TrigName.TANH, 0, false);
        
        // COTH
        param(pb, 0, Double.POSITIVE_INFINITY, TrigName.COTH, 0, true); // expect overflow exception
        param(pb, 1, (Math.E * Math.E + 1) / (Math.E * Math.E - 1), TrigName.COTH, 0, false);
    
        return pb.asList();
    }
       
    private static void param(ParameterizationBuilder pb, double input1,
            double expected, TrigExpression.TrigName name, double input2, boolean expectExc)
    {
        pb.add(name.toString() + " " + input1 + "_" + input2,
                input1, expected, name, input2, expectExc);
    }

    @OnlyIfNot("expectException()")
    @Test
    public void testWithoutExc()
    {
        test();
    }

    @OnlyIf("expectException()")
    @Test (expected = OverflowException.class)
    public void testWithExc ()
    {
        test();
    }

    public boolean expectException ()
    {
        return expectExc;
    }

    private void test()
    {
        Expression trigExpression = getTrigExpression();
        ValueSource result = trigExpression.evaluation().eval();
        double actual = result.getDouble();

        assertEquals(expected, actual,0.01);
        alreadyExc = true;
    }
    private static List <? extends Expression> getArgList (Expression ...arg)
    {
        return Arrays.asList(arg);
    }
    
    private  Expression getTrigExpression ()
    {       
            return new TrigExpression(
               ( name.equals(TrigExpression.TrigName.ATAN2)
                ? getArgList(ExprUtil.lit(input1), ExprUtil.lit(input2))
                : getArgList(ExprUtil.lit(input1))), name);
    }
    
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {        
        return new CompositionTestInfo(name.equals(TrigExpression.TrigName.ATAN2) ? 2 : 1, AkType.DOUBLE, true);
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        switch(name)
        {
            case SIN: return TrigExpression.SIN_COMPOSER;
            case COS: return TrigExpression.COS_COMPOSER;
            case TAN: return TrigExpression.TAN_COMPOSER;
            case COT: return TrigExpression.COT_COMPOSER;
            case ASIN: return TrigExpression.ASIN_COMPOSER;
            case ACOS: return TrigExpression.ACOS_COMPOSER;
            case ATAN: return TrigExpression.ATAN_COMPOSER;
            case ATAN2: return TrigExpression.ATAN2_COMPOSER;
            case COSH:  return TrigExpression.COSH_COMPOSER;
            case SINH:  return TrigExpression.SINH_COMPOSER;
            case TANH:  return TrigExpression.TANH_COMPOSER;
            default: return TrigExpression.COTH_COMPOSER;
        }
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
