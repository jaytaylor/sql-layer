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
        LongExtractor ex = Extractors.getLongExtractor(AkType.DATE);
        Expression l = new LiteralExpression(AkType.DATE,ex.getLong("2011-12-05"));
        Expression r = new LiteralExpression(AkType.DATE, ex.getLong("2011-11-01"));

        Expression top = DateTimeArithExpression.DATEDIFF_COMPOSER.compose(Arrays.asList(l,r));
        long actual = top.evaluation().eval().getLong();

        assertEquals(34L, actual);
    }

    @Test
    public void testNullDateiff()
    {
        Expression l = LiteralExpression.forNull();
        Expression r = new LiteralExpression(AkType.DATE, 1234L);

        Expression top = DateTimeArithExpression.DATEDIFF_COMPOSER.compose(Arrays.asList(l, r));
        assertTrue("top is null", top.evaluation().eval().isNull());
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
}
