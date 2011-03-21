package com.akiban.qp;

public enum Comparison
{
    EQ("=="),
    NE("!="),
    LT("<"),
    LE("<="),
    GT(">"),
    GE(">=");

    public String toString()
    {
        return symbol;
    }

    private Comparison(String symbol)
    {
        this.symbol = symbol;
    }

    private final String symbol;
}
