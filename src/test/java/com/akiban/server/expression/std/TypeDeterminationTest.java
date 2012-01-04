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

import java.util.EnumSet;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import org.junit.runner.RunWith;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class TypeDeterminationTest
{

    private AkType input1;
    private AkType input2;
    private AkType expected;
    private ArithOp op;

    public TypeDeterminationTest (AkType input1, ArithOp op, AkType input2, AkType expected)
    {
        this.input1 = input1;
        this.input2 = input2;
        this.op = op;
        this.expected = expected;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // ------------------ numeric types only -------------------------------
        for (ArithOp op : Arrays.asList(ArithOps.ADD, ArithOps.MINUS,
                ArithOps.DIVIDE, ArithOps.MOD, ArithOps.MULTIPLY))
        {
            // decimal
            paramNonSym(pb, AkType.DECIMAL, op, AkType.DECIMAL, AkType.DECIMAL);
            paramSym(pb, AkType.DECIMAL, op, AkType.DOUBLE, AkType.DECIMAL);
            paramSym(pb, AkType.DECIMAL, op, AkType.FLOAT, AkType.DECIMAL);
            paramSym(pb, AkType.DECIMAL, op, AkType.U_BIGINT, AkType.DECIMAL);
            paramSym(pb, AkType.DECIMAL, op, AkType.LONG, AkType.DECIMAL);
            paramSym(pb, AkType.DECIMAL, op, AkType.INT, AkType.DECIMAL);

            // double
            paramNonSym(pb, AkType.DOUBLE, op, AkType.DOUBLE, AkType.DOUBLE);
            paramSym(pb, AkType.DOUBLE, op, AkType.FLOAT, AkType.DOUBLE);
            paramSym(pb, AkType.DOUBLE, op, AkType.U_BIGINT, AkType.DOUBLE);
            paramSym(pb, AkType.DOUBLE, op, AkType.LONG, AkType.DOUBLE);
            paramSym(pb, AkType.DOUBLE, op, AkType.INT, AkType.DOUBLE);

            // float
            paramNonSym(pb, AkType.FLOAT, op, AkType.FLOAT, AkType.FLOAT);
            paramSym(pb, AkType.FLOAT, op, AkType.U_BIGINT, AkType.FLOAT);
            paramSym(pb, AkType.FLOAT, op, AkType.LONG, AkType.FLOAT);
            paramSym(pb, AkType.FLOAT, op, AkType.INT, AkType.FLOAT);

            // u_bigint
            paramNonSym(pb, AkType.U_BIGINT, op, AkType.U_BIGINT, AkType.U_BIGINT);
            paramSym(pb, AkType.U_BIGINT, op, AkType.LONG, AkType.U_BIGINT);
            paramSym(pb, AkType.U_BIGINT, op, AkType.INT, AkType.U_BIGINT);

            // long
            paramNonSym(pb, AkType.LONG, op, AkType.LONG, AkType.LONG);
            paramSym(pb, AkType.LONG, op, AkType.INT, AkType.LONG);

            // int
            paramNonSym(pb, AkType.INT, op, AkType.INT, AkType.INT);

        }

        // ------------------ date/time types only -------------------------------
        for (AkType datetime : Arrays.asList(AkType.DATE, AkType.DATETIME,
                                             AkType.TIME, AkType.TIMESTAMP, AkType.YEAR))
        {
            paramNonSym(pb, datetime, ArithOps.MINUS, datetime, AkType.INTERVAL_MILLIS);
            paramNonSym(pb,datetime, ArithOps.MINUS, AkType.INTERVAL_MILLIS, datetime);
            paramNonSym(pb, datetime, ArithOps.MINUS, AkType.INTERVAL_MONTH, datetime);
            paramSym(pb, datetime, ArithOps.ADD, AkType.INTERVAL_MILLIS, datetime);
            paramSym(pb,datetime, ArithOps.ADD, AkType.INTERVAL_MONTH,datetime);

            paramNonSym(pb, AkType.INTERVAL_MONTH, ArithOps.MINUS, datetime, null); // expect exception
            paramNonSym(pb, AkType.INTERVAL_MILLIS, ArithOps.MINUS, datetime, null); // expect exception

            for (ArithOp op : Arrays.asList(ArithOps.ADD, ArithOps.DIVIDE,
                                            ArithOps.MOD, ArithOps.MULTIPLY))
                paramNonSym(pb, datetime, op, datetime, null); // expect exception
        }
        
        // INTERVALs
        for (AkType interval : Arrays.asList(AkType.INTERVAL_MILLIS, AkType.INTERVAL_MONTH))
        {

            paramNonSym(pb, interval, ArithOps.ADD, interval, interval);
            paramNonSym(pb, interval, ArithOps.MINUS, interval, interval);
            paramNonSym(pb, interval, ArithOps.DIVIDE, interval, null); // expect exception
            paramNonSym(pb, interval, ArithOps.MULTIPLY, interval, null); // expect exception

            for (AkType numeric : Arrays.asList(AkType.LONG, AkType.DECIMAL, AkType.DOUBLE,
                    AkType.U_BIGINT, AkType.FLOAT, AkType.INT))
            {
                paramSym(pb, interval, ArithOps.MULTIPLY, numeric, interval);
                paramNonSym(pb, numeric, ArithOps.DIVIDE, interval, null); // expect exception
            }
        }
        
        // INTERVAL_MONTH and INTERVAL_MILLIS
        for (ArithOp op : Arrays.asList(ArithOps.ADD, ArithOps.MINUS,
                ArithOps.DIVIDE, ArithOps.MOD, ArithOps.MULTIPLY))
            paramSym(pb, AkType.INTERVAL_MILLIS, op, AkType.INTERVAL_MONTH, null); // expect exception

        // mixing date/times
        EnumSet<AkType> DATES = EnumSet.of
                (AkType.DATE, AkType.DATETIME, AkType.TIME, AkType.TIMESTAMP, AkType.YEAR);

        for (AkType type1 : DATES)
        {
            DATES.remove(type1);
            for (AkType type2 : DATES)
                for (ArithOp op : Arrays.asList(ArithOps.ADD, ArithOps.MINUS,
                            ArithOps.DIVIDE, ArithOps.MOD, ArithOps.MULTIPLY))
                    paramSym(pb, type1, op, type2, null);            
        }
        
        return pb.asList();
    }

    private static void paramNonSym(ParameterizationBuilder pb, AkType input1, ArithOp op, AkType input2, AkType expected)
    {
        pb.add(input1.name() + op.opName() + input2.name(), input1, op, input2, expected);
    }

    private static void paramSym(ParameterizationBuilder pb, AkType input1, ArithOp op, AkType input2, AkType expected)
    {
        pb.add(input1.name() + op.opName() + input2.name(), input1, op, input2, expected);
        pb.add(input2.name() + op.opName() + input1.name(), input2, op, input1, expected);
    }

    @OnlyIfNot("exceptionExpected()")
    @Test
    public void test ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, op, right);
        top.evaluation().eval();
        assertEquals(expected, top.topT);
    }

    @OnlyIf("exceptionExpected()")
    @Test (expected = InvalidArgumentTypeException.class)
    public void testWithException ()
    {
        Expression left = getExp(input1);
        Expression right = getExp(input2);
        ArithExpression top = new ArithExpression(left, op, right);
    }

    private Expression getExp (AkType type)
    {
        switch (type)
        {
            case FLOAT: return new LiteralExpression(type, 1.0f);
            case DOUBLE: return new LiteralExpression(type, 1.0);
            case DECIMAL: return new LiteralExpression(type, BigDecimal.ONE);
            case U_BIGINT: return new LiteralExpression(type, BigInteger.ONE);
            case INT:
            case LONG:
            case YEAR:
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case TIME:
            case INTERVAL_MONTH:
            case INTERVAL_MILLIS: return new LiteralExpression(type, 1L);
            case VARCHAR: return new LiteralExpression(type, "1");
            case NULL: return new LiteralExpression(type, null);
            default: return new LiteralExpression(AkType.UNSUPPORTED, null);
        }
    }

    public boolean exceptionExpected ()
    {
        return expected == null;
    } 
}
