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

public enum Comparison
{
    EQ("==", false, true, false),
    LT("<", true, false, false),
    LE("<=", true, true, false),
    GT(">", false, false, true),
    GE(">=", false, true, true),
    NE("!=", true, false, true)
    ;

    public boolean matchesCompareTo(int compareTo) {
        if (compareTo < 0)
            return includesLT;
        else if (compareTo > 0)
            return includesGT;
        return includesEQ;
    }

    public String toString()
    {
        return symbol;
    }

    private Comparison(String symbol, boolean lt, boolean eq, boolean gt)
    {
        this.symbol = symbol;
        this.includesLT = lt;
        this.includesEQ = eq;
        this.includesGT = gt;
    }

    private final String symbol;
    private final boolean includesLT;
    private final boolean includesEQ;
    private final boolean includesGT;
}