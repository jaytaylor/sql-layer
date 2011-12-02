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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicExpressionCompositionTest extends ComposedExpressionTestBase {

    private static boolean alreadyExc = false;
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        return Arrays.asList(
                Parameterization.create("AND", BoolLogicExpression.andComposer),
                Parameterization.create("OR", BoolLogicExpression.orComposer)
        );
    }

    @Test
    public void testdummy ()
    {
        alreadyExc = true;
    }
    @Override
    protected CompositionTestInfo getTestInfo (){
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() {
        return composer;
    }

    public BoolLogicExpressionCompositionTest(ExpressionComposer composer) {
        this.composer = composer;
    }

    private final ExpressionComposer composer;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.BOOL, true);

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
