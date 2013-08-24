/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import java.util.Arrays;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.junit.Parameterization;
import java.util.Collection;
import org.junit.runner.RunWith;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import org.junit.Test;

import static com.foundationdb.server.expression.std.IsExpression.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class IsExpressionTest extends ComposedExpressionTestBase
{
    private Expression arg;
    private boolean expected;
    ExpressionComposer composer;

    private static boolean alreadyExc = false;
    private static final Boolean [] ARGS = new Boolean [] {true, false, null};

    public IsExpressionTest (Expression arg, boolean expected, ExpressionComposer composer)
    {
        this.arg = arg;
        this.expected = expected;
        this.composer = composer;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params ()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // is true
        param(pb, new boolean [] {true, false, false}, IS_TRUE);

        // is false
        param(pb, new boolean [] {false, true, false}, IS_FALSE);

        // is unknown
        param(pb, new boolean [] {false, false, true}, IS_UNKNOWN);

        return pb.asList();
    }

    private static void param (ParameterizationBuilder pb, boolean expected[], ExpressionComposer composer)
    {
        for (int n = 0; n < 3; ++n)
            pb.add (ARGS[n] + " " + composer + " --->" + expected[n],
                    LiteralExpression.forBool(ARGS[n]), expected[n], composer);
    }

    @Test
    public void test ()
    {
        Expression top = compose(composer, Arrays.asList(arg));

        assertTrue("Top is BOOL", top.valueType() == AkType.BOOL);
        assertEquals(expected, top.evaluation().eval().getBool());
        alreadyExc = true;
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.BOOL, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return composer;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }    
}
