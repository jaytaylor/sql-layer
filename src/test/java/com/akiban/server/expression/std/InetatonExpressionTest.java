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
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InetatonExpressionTest  extends ComposedExpressionTestBase
{
    @Test
    public void test4Quads ()
    {
        test("10.0.5.9", 167773449);
    }

     @Test
    public void test3Quads ()
    {
        test("127.1.1", 2130771969); // equivalent to 127.1.0.1
    }

    @Test
    public void test2Quads ()
    {
        test("127.1", 2130706433); // equivalent to 127.0.0.1
    }

    @Test
    public void test1Quad()
    {
        test("127", 127); // equivalent to 0.0.0.127
    }

    @Test
    public void testZeroQuad()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, ""));
    }

    @Test
    public void test5Quads() // do not accept IPv6 or anything other than Ipv4
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR,"1.2.3.4.5"));
    }

    @Test
    public void testNull ()
    {
        testExpectNull(new LiteralExpression(AkType.NULL, null));
    }

    @Test
    public void testBadFormatString ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "12sdfa"));
    }

    @Test
    public void testNonNumeric ()
    {        
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "a.b.c.d"));
    }

    @Test
    public void testNeg ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "-127.0.0.1"));
    }

    @Test
    public void testNumberOutofRange ()
    {
        testExpectNull(new LiteralExpression(AkType.VARCHAR, "258.0.1.0"));
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

    //------------ private methods-------------------------
    private void testExpectNull (Expression arg)
    {
        assertTrue((new InetatonExpression(arg)).evaluation().eval().isNull());
    }
    private void test (String ip, long expected)
    {
        assertEquals(expected,
                (new InetatonExpression(new LiteralExpression(AkType.VARCHAR, ip))).evaluation().eval().getLong());
    }
}
