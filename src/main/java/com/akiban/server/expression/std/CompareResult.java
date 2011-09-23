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

import java.util.EnumSet;

enum CompareResult {
    LT(Comparison.LT, Comparison.LE, Comparison.NE),
    EQ(Comparison.EQ, Comparison.LE, Comparison.GE),
    GT(Comparison.GT, Comparison.GE, Comparison.NE)
    ;

    public boolean checkAgainstComparison(Comparison comparison) {
        return trueComparisons.contains(comparison);
    }

    CompareResult(Comparison trueComparison, Comparison... rest) {
        trueComparisons = EnumSet.of(trueComparison, rest);
    }

    private final EnumSet<Comparison> trueComparisons;
}
