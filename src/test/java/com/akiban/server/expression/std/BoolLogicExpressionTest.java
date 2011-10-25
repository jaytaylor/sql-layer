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
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicExpressionTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        // OR logic
        pb.add("||", BoolLogicExpression.orComposer, true, ERR, true);

        pb.add("||", BoolLogicExpression.orComposer, false, TRUE, true);
        pb.add("||", BoolLogicExpression.orComposer, false, FALSE, false);
        pb.add("||", BoolLogicExpression.orComposer, false, NULL, null);

        pb.add("||", BoolLogicExpression.orComposer, null, TRUE, true);
        pb.add("||", BoolLogicExpression.orComposer, null, FALSE, null);
        pb.add("||", BoolLogicExpression.orComposer, null, NULL, null);

        // AND logic
        pb.add("&&", BoolLogicExpression.andComposer, true, TRUE, true);
        pb.add("&&", BoolLogicExpression.andComposer, true, FALSE, false);
        pb.add("&&", BoolLogicExpression.andComposer, true, NULL, null);

        pb.add("&&", BoolLogicExpression.andComposer, false, ERR, false);

        pb.add("&&", BoolLogicExpression.andComposer, null, TRUE, null);
        pb.add("&&", BoolLogicExpression.andComposer, null, FALSE, false);
        pb.add("&&", BoolLogicExpression.andComposer, null, NULL, null);

        for (Parameterization param : pb.asList()) {
            Boolean a = (Boolean)param.getArgsAsList().get(1);
            Expression b = (Expression)param.getArgsAsList().get(2);
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

    private static String name(Expression b) {
        if (b == TRUE)
            return "T";
        if (b == FALSE)
            return "F";
        if (b == NULL)
            return "?";
        if (b == ERR)
            return "x";
        throw new RuntimeException("unknown expression: " + b);
    }

    @Test
    public void test() {
        Expression test = composer.compose(Arrays.asList(lhs, rhs));
        Boolean actual = Extractors.getBooleanExtractor().getBoolean(test.evaluation().eval(), null);
        assertEquals(expected, actual);
    }

    public BoolLogicExpressionTest(ExpressionComposer composer, Boolean lhs, Expression rhs, Boolean expected) {
        this.composer = composer;
        this.lhs = LiteralExpression.forBool(lhs);
        this.rhs = rhs;
        this.expected = expected;
    }

    private final ExpressionComposer composer;
    private final Expression lhs;
    private final Expression rhs;
    private final Boolean expected;

    private static final Expression TRUE = LiteralExpression.forBool(true);
    private static final Expression FALSE = LiteralExpression.forBool(false);
    private static final Expression NULL = LiteralExpression.forBool(null);
    private static final Expression ERR = ExprUtil.exploding(AkType.BOOL);
}
