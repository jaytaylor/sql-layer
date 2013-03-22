
package com.akiban.server.expression.std;

public enum Comparison
{
    
    EQ("==", false, true, false),
    GE(">=", false, true, true),
    GT(">", false, false, true),
    LE("<=", true, true, false),
    LT("<", true, false, false),
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