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

import com.akiban.junit.OnlyIf;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.Test;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.ExprUtil.*;
import static com.akiban.server.types.AkType.*;

@RunWith(NamedParameterizedRunner.class)
public class FieldFunctionExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    
    private List<? extends Expression> args;
    private Long expected;
    
    public FieldFunctionExpressionTest (List<? extends Expression> args, Long expected)
    {
        this.args = args;
        this.expected = expected;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        // easy cases: all args have the same type
        param(pb, 0L, lit(2L), lit(1L));
        param(pb, 4L, lit(3.6), lit(3.0), lit(3.59), lit(3.7), lit(3.6));
        param(pb, 4L, lit("00.1"), lit("666"), lit("0.1"), lit("777"), lit("00.1"));
        param(pb, 0L, lit(true), lit(false));
        param(pb, 2L, new LiteralExpression(DATETIME, 20091107123010L),
                      new LiteralExpression(DATETIME, 20091107123009L),
                      new LiteralExpression(DATETIME, 20091107123010L),
                      new LiteralExpression(DATETIME, 20091107123011L));
        
        param(pb, 0L, new LiteralExpression(TIME, 123010L),
                      new LiteralExpression(TIME, 123011L));
        
        // heterogeneous types
        param(pb, 3L, lit(123), LiteralExpression.forNull(), lit("123.1"), lit(123));
        param(pb, 2L, lit("00.1"), lit("666"), lit("0.1"), lit("777"), lit(true));
        param(pb, 0L, lit("00.1"), lit("666"), lit("0.11"), lit("777"), lit(true));
        param(pb, 2L, lit("12:30:10"), lit(123010L), new LiteralExpression(TIME, 123010L));
        param(pb, 3L, new LiteralExpression(TIME, 123010L), 
                      lit(12345L),
                      lit("123010.1"),
                      lit("123010"));
        param(pb, 2L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit("00:00:01"));
        param(pb, 0L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit("00:00:02"));
        param(pb, 2L, new LiteralExpression(INTERVAL_MILLIS, 1000L),
                      lit(1000.2),
                      lit(1000L));
        
        param(pb, 1L, new LiteralExpression(DATETIME, 19910510073010L),
                      lit ("19910510073010"));
        
        param(pb, 1L, new LiteralExpression(DATETIME, 19910510073010L),
                      lit("1991-05-10 07:30:10"));
        
        // this returns 0L
        // but probably should returns 1L
        param(pb, 0L, lit("1991-05-10 07:30:10"),
                      lit("19910510073010"),
                      lit(1L));
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, Long exp, Expression...args)
    {
        List<? extends Expression> argsList = Arrays.asList(args);
        pb.add("FIELD("+ argsList +") expcted: " + (exp == null ? "NULL" : exp),
                argsList,
                exp);
    }
    
    @OnlyIfNot("expectArityException()")
    @Test
    public void test()
    {
        doTest();
    }
    
    @OnlyIf("expectArityException()")
    @Test(expected=WrongExpressionArityException.class)
    public void testArity()
    {
        doTest();
    }
    
    private void doTest()
    {
        alreadyExc = true;
        Expression top = new FieldFunctionExpression(args);
        assertEquals(expected.longValue(), top.evaluation().eval().getLong());
    }
    
    public boolean expectArityException ()
    {
        return expected == null;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, VARCHAR, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return FieldFunctionExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
