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
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class AndExpressionTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        pb.add("", true, true, true);
        pb.add("", true, false, false);
        pb.add("", true, null, null);

        pb.add("", false, true, false);
        pb.add("", false, false, false);
        pb.add("", false, null, false);

        pb.add("", null, true, null);
        pb.add("", null, false, false);
        pb.add("", null, null, null);

        for (Parameterization param : pb.asList()) {
            Boolean a = (Boolean)param.getArgsAsList().get(0);
            Boolean b = (Boolean)param.getArgsAsList().get(1);
            Boolean r = (Boolean)param.getArgsAsList().get(2);
            param.setName(String.format("%s%s -> %s", name(a), name(b), name(r)));
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
        Expression test = new AndExpression(Arrays.asList(left, right));
        assertTrue("test should be const", test.isConstant());
        Boolean actual = Extractors.getBooleanExtractor().getBoolean(test.evaluation().eval(), null);
        assertEquals(expected, actual);
    }

    public AndExpressionTest(Boolean one, Boolean two, Boolean expected) {
        this.one = one;
        this.two = two;
        this.expected = expected;
    }

    private final Boolean one;
    private final Boolean two;
    private final Boolean expected;
}
