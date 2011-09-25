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

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class ComparisonTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        param(pb, Comparison.EQ, false, true, false);
        param(pb, Comparison.LT, true, false, false);
        param(pb, Comparison.LE, true, true, false);
        param(pb, Comparison.GT, false, false, true);
        param(pb, Comparison.GE, false, true, true);
        param(pb, Comparison.NE, true, false, true);

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb,
                              Comparison comparison, boolean againstLt, boolean againstEq, boolean againstGt)
    {
        pb.add(comparison.name(), comparison, againstLt, againstEq, againstGt);
    }

    @Test
    public void againstLt() {
        test(-1, againstLt);
    }

    @Test
    public void againstEq() {
        test(0, againstEq);
    }

    @Test
    public void againstGt() {
        test(1, againstGt);
    }

    public ComparisonTest(Comparison comparison, boolean againstLt, boolean againstEq, boolean againstGt) {
        this.comparison = comparison;
        this.againstLt = againstLt;
        this.againstEq = againstEq;
        this.againstGt = againstGt;
    }

    private void test(int compareToResult, boolean expectedResult) {
        assertEquals(expectedResult, comparison.matchesCompareTo(compareToResult));
    }

    private final Comparison comparison;
    private final boolean againstLt;
    private final boolean againstEq;
    private final boolean againstGt;
}
