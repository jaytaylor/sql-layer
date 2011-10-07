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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import static com.akiban.server.expression.std.Comparison.*;
import static com.akiban.server.expression.std.ExprUtil.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class CompareExpressionTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // null to null
        param(pb, constNull(), constNull(), NULL);

        // longs
        param(pb, constNull(), lit(5), NULL);
        param(pb, lit(5), constNull(), NULL);
        param(pb, lit(5), lit(5), LE, EQ, GE);
        param(pb, lit(4), lit(5), LE, LT, NE);
        param(pb, lit(5), lit(4), GE, GT, NE);

        // doubles
        param(pb, constNull(), lit(5.0), NULL);
        param(pb, lit(5.0), constNull(), NULL);
        param(pb, lit(5.0), lit(5.0), LE, EQ, GE);
        param(pb, lit(4.0), lit(5.0), LE, LT, NE);
        param(pb, lit(5.0), lit(4.0), GE, GT, NE);
        
        // String
        param(pb, constNull(), lit("alpha"), NULL);
        param(pb, lit("beta"), constNull(), NULL);
        param(pb, lit("aaa"), lit("aaa"), LE, EQ, GE);
        param(pb, lit("aa"), lit("aaa"), LE, LT, NE);
        param(pb, lit("aaa"), lit("aa"), GE, GT, NE);

        // bools
        param(pb, constNull(), lit(true), NULL);
        param(pb, lit(true), constNull(), NULL);
        param(pb, lit(true), lit(true), LE, EQ, GE);
        param(pb, lit(false), lit(false), LE, EQ, GE);
        param(pb, lit(false), lit(true), LE, LT, NE);
        param(pb, lit(true), lit(false), GE, GT, NE);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, Expression left, Expression right, Comparison... trues) {
        EnumSet<Comparison> truesSet = EnumSet.noneOf(Comparison.class);
        Collections.addAll(truesSet, trues);
        for (Comparison comparison : Comparison.values()) {
            String name = String.format("%s %s %s", left, comparison, right);
            Boolean expected = trues == NULL ? null : truesSet.contains(comparison);
            pb.add(name, left, right, comparison, expected);
        }
    }

    @Test
    public void test() {
        Expression compareExpression = new CompareExpression(left, comparison, right);
        assertEquals("compareExpression type", AkType.BOOL, compareExpression.valueType());
        assertFalse("compareExpression needs row", compareExpression.needsRow());
        assertFalse("compareExpression needs bindings", compareExpression.needsBindings());
        ExpressionEvaluation evaluation = compareExpression.evaluation();
        ValueSource source = evaluation.eval();
        if (expected == null) {
            assertTrue("source should have been null: " + source, source.isNull());
        }
        else {
            assertEquals("comparison", expected, source.getBool());
        }
    }

    public CompareExpressionTest(Expression left, Expression right, Comparison comparison, Boolean expected) {
        this.left = left;
        this.right = right;
        this.comparison = comparison;
        this.expected = expected;
    }

    private final Expression left;
    private final Expression right;
    private final Comparison comparison;
    private final Boolean expected;

    private static final Comparison[] NULL = new Comparison[0];
}
