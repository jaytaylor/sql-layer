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

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
public class InetatonExpressionTest  extends ComposedExpressionTestBase
{
    @Test
    public void testLongForm ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "10.0.5.9");
        InetatonExpression ip = new InetatonExpression(arg);
        ValueSource s = ip.evaluation().eval();
        long actual = s.getLong();

        assertEquals(167773449, actual);
    }

    @Test
    public void testShortForm ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "127.1");
        InetatonExpression ip = new InetatonExpression(arg);
        ValueSource s = ip.evaluation().eval();
        long actual = s.getLong();

        assertEquals(2130706433,actual);
    }

    @Test
    public void testNull ()
    {
        Expression arg = new LiteralExpression(AkType.NULL, null);
        InetatonExpression ip = new InetatonExpression(arg);
        ValueSource s = ip.evaluation().eval();

        assertTrue(s.isNull());
    }

    @Test
    public void testBadFormatString ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "12sdfa");
        InetatonExpression ip = new InetatonExpression(arg);
        ValueSource s = ip.evaluation().eval();

        assertTrue(s.isNull());
    }

    @Test
    public void testNonNumeric ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "a.b.c.d");
        InetatonExpression ip = new InetatonExpression(arg);
        ValueSource s = ip.evaluation().eval();

        assertTrue(s.isNull());
    }

    @Override
    protected int childrenCount()
    {
        return 1;
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return InetatonExpression.COMPOSER;
    }
}
