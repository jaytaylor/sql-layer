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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;

public class YearWeekExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void testFirstDay()
    {
        // test first day
        String st = "2009-1-1";
        test(st,0, 200852);
        test(st,1,200901);
        test(st,2,200852);
        test(st,3,200901);
        test(st,4,200853);
        test(st,5,200852);
        test(st,6,200853);
        test(st,7,200852);

        // test last day
        for (int i = 0; i < 8; ++i)
            test("2012-12-31",i,201253);

        // test 2010-may-08
        test(st = "2011-5-8",7,201118);
        test(st,6,201119);
        test(st,5,201118);
        test(st,4,201119);
        test(st,3,201118);
        test(st,2,201119);
        test(st,1,201118);
        test(st,0,201119);
    }

    @Test
    public void testNullFirst ()
    {
        Expression yearWeek = new YearWeekExpression(Arrays.asList(LiteralExpression.forNull(),
                new LiteralExpression(AkType.LONG, 4)));
        assertEquals(AkType.INT,yearWeek.valueType());
        assertTrue(yearWeek.evaluation().eval().isNull());
    }

    @Test
    public void testNullSecond ()
    {
        Expression yearWeak = new YearWeekExpression(Arrays.asList(new LiteralExpression(AkType.DATE, 12345L),
                LiteralExpression.forNull()));
        assertEquals(AkType.INT,yearWeak.valueType());
        assertTrue(yearWeak.evaluation().eval().isNull());
    }

    @Test (expected = WrongExpressionArityException.class)
    public void testWrongArity()
    {
        Expression yearWeek = new YearWeekExpression(Arrays.asList(new LiteralExpression(AkType.DATE, 12345L),
                new LiteralExpression(AkType.INT, 4),
                new LiteralExpression(AkType.INT, 4)));
    }

    @Test (expected = InvalidParameterValueException.class)
    public void testZeroYear()
    {
        test("0000-12-2", 0, 0);
    }

    @Test (expected = InvalidParameterValueException.class)
    public void testZeroMonth()
    {
        test("0001-00-02", 0, 0);
    }

    @Test (expected = InvalidParameterValueException.class)
    public void testZeroDay()
    {
        test("0001-02-00", 0, 0);
    }

    @Test (expected = InvalidParameterValueException.class)
    public void testInvalidMode()
    {
        test("2009-12-2", 10, 0);
    }

    private void test(String dateS, int mode, int exp)
    {
        long date = Extractors.getLongExtractor(AkType.DATE).getLong(dateS);

        Expression d = new LiteralExpression(AkType.DATE, date);
        Expression m = new LiteralExpression(AkType.INT, mode);
        Expression yearWeek = new YearWeekExpression(Arrays.asList(d, m));

        int actual = (int) yearWeek.evaluation().eval().getInt();
        assertEquals("assert topType is INT", AkType.INT, yearWeek.valueType());
        assertEquals("DATE: " + date + ", mode " + mode, actual, exp);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return YearWeekExpression.WEEK_COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
