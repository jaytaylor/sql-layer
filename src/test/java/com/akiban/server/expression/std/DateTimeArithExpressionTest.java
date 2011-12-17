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

import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;

public class DateTimeArithExpressionTest extends ComposedExpressionTestBase
{
    @Test
    public void testTimeDiff ()
    {
        Expression l = new LiteralExpression(AkType.TIME,100915L);
        Expression r = new LiteralExpression(AkType.TIME, 90915L);

        Expression top = DateTimeArithExpression.TIMEDIFF_COMPOSER.compose(Arrays.asList(l,r));
        long actual = top.evaluation().eval().getTime();

        assertEquals(10000L, actual);
    }

    @Test
    public void testDateTimeDiff()
    {
        Expression l = new LiteralExpression(AkType.DATETIME, 20091010123010L);
        Expression r = new LiteralExpression(AkType.DATETIME, 20091010123001L);

        Expression top = DateTimeArithExpression.TIMEDIFF_COMPOSER.compose(Arrays.asList(r,l));
        long actual = top.evaluation().eval().getTime();

        assertEquals(-9L, actual);
    }

    @Test
    public void testTimeStampDiff ()
    {
        Expression l = new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-10 10:10:10"));
        Expression r = new LiteralExpression(AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-11 10:09:09"));

        Expression top = DateTimeArithExpression.TIMEDIFF_COMPOSER.compose(Arrays.asList(l,r));
        long actual = top.evaluation().eval().getTime();

        assertEquals(-235859L, actual);
    }

    @Test
    public void testNullTimeDiff ()
    {
        Expression l = LiteralExpression.forNull();
        Expression r = new LiteralExpression(AkType.TIME, 1234L);

        Expression top = DateTimeArithExpression.TIMEDIFF_COMPOSER.compose(Arrays.asList(l,r));
        assertTrue ("top is null", top.evaluation().eval().isNull());
    }

    @Test
    public void testDateDiff ()
    {
        test("2011-12-05", "2011-11-01", 34L);
        test("2009-12-10", "2010-01-12", -33L);
        test("2008-02-01", "2008-03-01", -29L);
        test("2010-01-01", "2010-01-01", 0L);
    }

    @Test
    public void testNullDateiff()
    {
        Expression l = LiteralExpression.forNull();
        Expression r = new LiteralExpression(AkType.DATE, 1234L);

        Expression top = DateTimeArithExpression.DATEDIFF_COMPOSER.compose(Arrays.asList(l, r));
        assertTrue("top is null", top.evaluation().eval().isNull());
    }

    @Test(expected = InvalidArgumentTypeException.class)
    public void testInvalidArgumenTypeDateDiff ()
    {
        Expression l = new LiteralExpression(AkType.DATE, 1234L);
        Expression r = new LiteralExpression(AkType.DATETIME, 1234L);

         Expression top = DateTimeArithExpression.DATEDIFF_COMPOSER.compose(Arrays.asList(l, r));
    }

    @Test(expected = InvalidArgumentTypeException.class)
    public void testInvalidArgumenTypeTimeDiff ()
    {
        Expression l = new LiteralExpression(AkType.DATE, 1234L);
        Expression r = new LiteralExpression(AkType.DATETIME, 1234L);

         Expression top = DateTimeArithExpression.TIMEDIFF_COMPOSER.compose(Arrays.asList(l, r));
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return DateTimeArithExpression.DATEDIFF_COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
    
    // ---------------- private method -----------------
        
    private static void test (String left, String right, long expected)
    {
        LongExtractor ex = Extractors.getLongExtractor(AkType.DATE);
        Expression l = new LiteralExpression(AkType.DATE,ex.getLong(left));
        Expression r = new LiteralExpression(AkType.DATE, ex.getLong(right));

        Expression top = DateTimeArithExpression.DATEDIFF_COMPOSER.compose(Arrays.asList(l,r));
        long actual = top.evaluation().eval().getLong();

        assertEquals(expected, actual);
    }
}
