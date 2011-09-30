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
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicCompositionTest extends ComposedExpressionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        return Arrays.asList(
                Parameterization.create("AND", BoolLogicExpression.AND_COMPOSER),
                Parameterization.create("OR", BoolLogicExpression.OR_COMPOSER)
        );
    }
    
    @Override
    protected int childrenCount() {
        return 2;
    }

    @Override
    protected Expression getExpression(List<? extends Expression> children) {
        return composer.compose(children);
    }

    public BoolLogicCompositionTest(ExpressionComposer composer) {
        this.composer = composer;
    }

    private final ExpressionComposer composer;
}
