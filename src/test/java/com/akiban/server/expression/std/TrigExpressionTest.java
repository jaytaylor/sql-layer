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

import com.akiban.server.expression.std.TrigExpression.TrigName;
import java.util.Random;
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
    
    private static Random rand = new Random();
   
    public TrigExpressionTest (double input, double expected, TrigExpression.TrigName name, double input2)
    {
        this.input1 = input;
        this.expected = expected;
        this.name = name;
        this.input2 = input2;        
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
 
        double x, y ;
        
        for (TrigName trName : TrigName.values())
        {   
            y = getRandomNumber ();
            x = getRandomNumber ();
           
            param(pb,x, getExpected(x,y,trName),trName, y);
        }
        
        return pb.asList();
    }
    
    private static double getExpected (double input1, double input2, TrigName name)
    {
        switch (name)
        {
            case SIN: return Math.sin(input1);
            case COS: return Math.cos(input1);
            case TAN: return Math.tan(input1);
            case COT: return Math.cos(input1) / Math.sin(input1);
            case ASIN: return Math.asin(input1);
            case ACOS: return Math.acos(input1);
            case ATAN: return Math.atan(input1);
            case ATAN2: return Math.atan2(input1, input2);
            default: return Math.cosh(input1);
        }
    }
    
    private static void param(ParameterizationBuilder pb, double input1,
            double expected, TrigExpression.TrigName name, double input2)
    {
        pb.add(name.toString() + " " + input1 + "_" + input2,
                input1, expected, name, input2);
    }
    
    private static double getRandomNumber ()
    {
        return rand.nextDouble() * Math.pow(10, rand.nextInt(3));
    }
    
    @Test
    public void test()
    {
        Expression trigExpression = getTrigExpression();     
        ValueSource result = trigExpression.evaluation().eval();
        double actual = result.getDouble();
        
        assertEquals(expected, actual,0.01);
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
    protected int childrenCount() 
    {
        
        return (name.equals(TrigExpression.TrigName.ATAN2) ? 2 : 1); // or 1? 
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
            default: return TrigExpression.COSH_COMPOSER;
        }
 
    }
    
}
