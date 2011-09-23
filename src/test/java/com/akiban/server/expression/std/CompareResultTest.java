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
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class CompareResultTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        param(pb, Comparison.EQ, false, true, false);
        param(pb, Comparison.LT, true, false, false);
        param(pb, Comparison.LE, true, true, false);
        param(pb, Comparison.GT, false, false, true);
        param(pb, Comparison.GE, false, true, true);
        param(pb, Comparison.NE, true, false, true);

        Set<String> requiredNames = new HashSet<String>();
        for (Comparison comparison : Comparison.values()) {
            for (CompareResult compareResult : CompareResult.values()) {
                boolean added = requiredNames.add(paramName(comparison, compareResult));
                assertTrue("duplicate found!", added);
            }
        }
        Set<String> actualNames = new HashSet<String>();
        for (Parameterization param : pb.asList()) {
            boolean added = actualNames.add(param.getName());
            assertTrue("duplicate found!", added);
        }
        assertEquals("required names", requiredNames, actualNames);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb,
                              Comparison comparison, boolean againstLt, boolean againstEq, boolean againstGt)
    {
        pb.add(paramName(comparison, CompareResult.EQ), comparison, CompareResult.EQ, againstEq);
        pb.add(paramName(comparison, CompareResult.LT), comparison, CompareResult.LT, againstLt);
        pb.add(paramName(comparison, CompareResult.GT), comparison, CompareResult.GT, againstGt);
    }

    private static String paramName(Comparison comparison, CompareResult compareResult) {
        return comparison + " against " + compareResult;
    }

    @Test
    public void test() {
        boolean actual = compareResult.checkAgainstComparison(comparison);
        assertEquals(expectedMatch, actual);
    }

    public CompareResultTest(Comparison comparison, CompareResult compareResult, boolean expectedMatch) {
        this.comparison = comparison;
        this.compareResult = compareResult;
        this.expectedMatch = expectedMatch;
    }

    private final Comparison comparison;
    private final CompareResult compareResult;
    private final boolean expectedMatch;
}
