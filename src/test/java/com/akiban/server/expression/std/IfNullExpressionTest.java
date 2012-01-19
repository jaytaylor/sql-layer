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
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith (NamedParameterizedRunner.class)
public class IfNullExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private boolean expectExc;
    private int nargs;

    public IfNullExpressionTest (int nargs, boolean expectExc)
    {
        this.nargs = nargs;
        this.expectExc = expectExc;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params ()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
        
        param(pb, 0, true);
        param(pb, 1, true);
        param(pb, 2, false);

        for (int n = 3; n < 20; ++n)
            param(pb, n, true);

        return pb.asList();
    }

    private static void param (ParameterizationBuilder pb, int nargs, boolean error)
    {
        pb.add("Children count = " + nargs + "---Expect WrongArityException? " + error, nargs, error);
    }

    @OnlyIf("expectExc()")
    @Test (expected=WrongExpressionArityException.class)
    public void testWithExc()
    {
        testArity(nargs);
    }

    @OnlyIfNot("expectExc()")
    @Test
    public void testWithoutExc()
    {
        testArity(nargs);
    }

    private static void testArity (int nargs)
    {
        alreadyExc = true;
        Expression arg = new LiteralExpression(AkType.LONG, 1);
        List<Expression> args = new ArrayList(nargs);
        for (int n = 0; n < nargs; ++n)
            args.add(arg);
        new IfNullExpression(args);
        
    }

    public boolean expectExc ()
    {
        return expectExc;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.LONG, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return IfNullExpression.IFNULL_COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
