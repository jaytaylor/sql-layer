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

package com.foundationdb.server.expression.std;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
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
        List<Expression> args = new ArrayList<>(nargs);
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
