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

import com.akiban.server.error.WrongExpressionArityException;
import java.util.List;
import java.util.ArrayList;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static com.akiban.server.types.AkType.*;

public final class CoalesceExpressionTest extends ComposedExpressionTestBase {
    
    @Test
    public void typeDetermination ()
    {
        // Test heterogeneous input types
         check(DOUBLE, // expect double
                 LONG, DOUBLE, FLOAT, U_BIGINT);
         check(VARCHAR, // expect VARCHAR
                 DATE, TIME, LONG, DOUBLE, BOOL);
         check(VARCHAR,
                 BOOL, NULL, LONG);
         check(INT,
                 INT, U_INT, INT, NULL);
         check(FLOAT,
                 LONG, U_INT, FLOAT);
         check(VARCHAR,
                 LONG, DOUBLE, VARCHAR);
         
         // Test homogeneous input types
         check(DOUBLE,
                 DOUBLE, NULL);
         check(LONG,
                 LONG, LONG);
         check(BOOL,
                 BOOL, BOOL, BOOL, NULL);
         check(DATE,
                 DATE, DATE,DATE);
         check(VARCHAR,
                 VARCHAR);
         check(NULL,
                 NULL);
         check(NULL,
                 NULL, NULL);
    }
    
    @Test
    public void test()
    {
        check(new ValueHolder(DOUBLE, 1.0),
                lit(NULL,0),
                lit(NULL, 2),
                lit(LONG, 1), 
                lit(LONG, 2),
                lit(FLOAT, 3),
                lit(DOUBLE, 4),
                lit(U_BIGINT,5));
        
        check(new ValueHolder(U_BIGINT, BigInteger.valueOf(1L)),
                lit(INT, 1),
                lit(U_INT, 2),
                lit(LONG, 3),
                lit(NULL, 0),
                lit(U_BIGINT, 4));
        
        check(new ValueHolder(U_BIGINT, BigInteger.valueOf(5L)),
                lit(U_BIGINT, 5));
        
        check(new ValueHolder(VARCHAR, "1.0"),
                lit(DOUBLE, 1.0),
                lit(BOOL, 2),
                lit(LONG, 3),
                lit(NULL, 4));
      
        check(new ValueHolder(VARCHAR, "true"),
                lit(BOOL, 1),
                lit(LONG, 2),
                lit(LONG, 3));
        
        check(new ValueHolder(AkType.VARCHAR, "2006-11-07 12:30:10"),
                lit(NULL, 0),
                lit(DATETIME, (double)20061107123010L),
                lit(DOUBLE, 2.0));
        
        check(new ValueHolder(NullValueSource.only()),
                lit(NULL, 0),
                lit(NULL, 1));
        
        check(new ValueHolder(NullValueSource.only()),
                lit(NULL, 0));
        
        check (new ValueHolder(BOOL, false),
                lit(NULL, 0),
                lit(BOOL, 0));
    }
    
    @Test(expected = WrongExpressionArityException.class)
    public void testExpArity ()
    {
        List<? extends Expression> arg = new ArrayList(0);
        new CoalesceExpression(arg);
    }
    
    private static void check (ValueHolder expected, Expression ... args)
    {
        Expression top = new CoalesceExpression(Arrays.asList(args));
        
        assertEquals("check top type ", expected.getConversionType(), top.valueType());
        assertEquals("check value ", expected, new ValueHolder(top.evaluation().eval()));
    }
    
    private static Expression lit (AkType type, double val)
    {
        switch(type)
        {
            case DATE:
            case DATETIME:
            case TIME:
            case YEAR:
            case LONG:  
            case U_INT:
            case INT:       return new LiteralExpression(type, (long)val);
            case DOUBLE:
            case U_DOUBLE:  return new LiteralExpression(type, val);
            case FLOAT:     return new LiteralExpression(type, (float)val);
            case U_BIGINT:  return new LiteralExpression(type, BigInteger.valueOf((long)val));
            case BOOL:      return LiteralExpression.forBool(val != 0 );
            case NULL:      return LiteralExpression.forNull();
            default:        throw new RuntimeException("unexpected type");
        }
    }
    
    private static Expression lit (AkType type, String st)
    {
        return new LiteralExpression(type, st);
    }
    
    private static void check (AkType expected, AkType ...types)
    {
        AkType actual = CoalesceExpression.getTopType(Arrays.asList(types));
        assertEquals(expected, actual);
    }

    // ComposedExpressionTestBase

    @Override
    protected ExpressionComposer getComposer() {
        return CoalesceExpression.COMPOSER;
    }

    @Override
    protected CompositionTestInfo getTestInfo () {
        return testInfo;
    }

    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.LONG, false);

    @Override
    public boolean alreadyExc()
    {
        return false;
    }
}
