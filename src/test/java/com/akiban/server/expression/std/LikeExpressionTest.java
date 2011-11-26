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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class LikeExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo info = new CompositionTestInfo(2, AkType.VARCHAR, true);
    private ExpressionComposer ex;
    private String left;
    private String right;
    private boolean expected;
    private boolean expectNull;

    protected static boolean already = false;
    public LikeExpressionTest (ExpressionComposer ex, String left, String right, boolean expected, boolean expectNull)
    {
        this.ex = ex;
        this.left = left;
        this.right = right;
        this.expected = expected;
        this.expectNull = expectNull;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        param(pb, LikeExpression.ILIKE_COMPOSER, "xyz", "XYZ%", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "xyz", "XYZ%", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "John Smith", "j%h", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "John Smith", "j%h", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "abc_", "%\\_", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "abc_", "%\\_", true, false);
        
        param(pb, LikeExpression.ILIKE_COMPOSER, "_abc", "_%", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "_abc", "_%", true, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "ab", "a\\%", false, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "ab", "a\\%", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "ab", "a\\_", false, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "ab", "a\\_", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "Ab", "a_", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "Ab", "a_", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "Ab", "A%", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "Ab", "A%", true, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "A%", "a\\%", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "A%", "a\\%", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "a_", "a\\_", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "a_", "a\\_", true, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "C:\\java\\", "_:%\\", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "C:\\java\\", "_:%\\", true, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "", "", true, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "", "", true, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "", "%", true,  false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "", "%", true,  false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "", "_", false, false);
        param(pb, LikeExpression.BLIKE_COMPOSER, "", "_", false, false);

        param(pb, LikeExpression.ILIKE_COMPOSER, "abc", "", false, true);
        param(pb, LikeExpression.BLIKE_COMPOSER, "abc", "", false, true);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, ExpressionComposer ex,
            String left, String right, boolean expected, boolean expectNull)
    {
        pb.add(left + " " + ex.toString() + " " + right, ex, left, right, expected, expectNull );
    }

    @Test
    public void test()
    {
        Expression l = new LiteralExpression(AkType.VARCHAR, left);
        Expression r;

        if (expectNull)
            r = LiteralExpression.forNull();
        else
            r = new LiteralExpression(AkType.VARCHAR, right);

        Expression top = getComposer().compose(Arrays.asList(l, r));

        if (expectNull)
            assertTrue(top.evaluation().eval().isNull());
        else
            assertTrue (top.evaluation().eval().getBool() == expected);
       
        already = true;
    }

    @Override
    public boolean alreadyExc ()
    {
        return already;
    }
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return info;
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ex;
    }
}
