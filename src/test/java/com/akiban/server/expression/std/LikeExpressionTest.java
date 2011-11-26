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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;


@RunWith(NamedParameterizedRunner.class)
public class LikeExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo info = new CompositionTestInfo(2, AkType.VARCHAR, true);
  //  private ExpressionComposer ex;
   // private String left;

    protected static boolean already = false;
    public LikeExpressionTest ()
    {

    }
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("test1");
        pb.add("test2");
        pb.add("test3");

        return pb.asList();
    }

    @Test
    public void dummyTest()
    {
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
        return LikeExpression.ILIKE_COMPOSER;
    }

}
