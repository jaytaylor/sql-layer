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
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicExpressionTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        // OR logic
        pb.add("||", BoolLogicExpression.orComposer, true, true, true);
        pb.add("||", BoolLogicExpression.orComposer, true, false, true);
        pb.add("||", BoolLogicExpression.orComposer, true, null, true);

        pb.add("||", BoolLogicExpression.orComposer, false, true, true);
        pb.add("||", BoolLogicExpression.orComposer, false, false, false);
        pb.add("||", BoolLogicExpression.orComposer, false, null, null);

        pb.add("||", BoolLogicExpression.orComposer, null, true, true);
        pb.add("||", BoolLogicExpression.orComposer, null, false, null);
        pb.add("||", BoolLogicExpression.orComposer, null, null, null);

        // AND logic
        pb.add("&&", BoolLogicExpression.andComposer, true, true, true);
        pb.add("&&", BoolLogicExpression.andComposer, true, false, false);
        pb.add("&&", BoolLogicExpression.andComposer, true, null, null);

        pb.add("&&", BoolLogicExpression.andComposer, false, true, false);
        pb.add("&&", BoolLogicExpression.andComposer, false, false, false);
        pb.add("&&", BoolLogicExpression.andComposer, false, null, false);

        pb.add("&&", BoolLogicExpression.andComposer, null, true, null);
        pb.add("&&", BoolLogicExpression.andComposer, null, false, false);
        pb.add("&&", BoolLogicExpression.andComposer, null, null, null);

        for (Parameterization param : pb.asList()) {
            Boolean a = (Boolean)param.getArgsAsList().get(1);
            Boolean b = (Boolean)param.getArgsAsList().get(2);
            Boolean r = (Boolean)param.getArgsAsList().get(3);
            param.setName(String.format("%s %s %s -> %s", name(a), param.getName(), name(b), name(r)));
        }
        return pb.asList();
    }

    private static String name(Boolean b) {
        if (b == null)
            return "?";
        return b ? "T" : "F";
    }

    @Test
    public void test() {
        Expression left = LiteralExpression.forBool(one);
        Expression right = LiteralExpression.forBool(two);
        Expression test = composer.compose(Arrays.asList(left, right));
        assertTrue("test should be const", test.isConstant());
        Boolean actual = Extractors.getBooleanExtractor().getBoolean(test.evaluation().eval(), null);
        assertEquals(expected, actual);
    }

    public BoolLogicExpressionTest(ExpressionComposer composer, Boolean one, Boolean two, Boolean expected) {
        this.composer = composer;
        this.one = one;
        this.two = two;
        this.expected = expected;
    }

    private final ExpressionComposer composer;
    private final Boolean one;
    private final Boolean two;
    private final Boolean expected;
}
