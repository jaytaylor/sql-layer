
package com.akiban.sql.optimizer.rule.range;

enum ComparisonResult {
    LT("<"),
    LT_BARELY("~<"),
    GT(">"),
    GT_BARELY("~>"),
    EQ("=="),
    INVALID("??")
    ;

    public ComparisonResult normalize() {
        switch (this) {
        case LT_BARELY: return LT;
        case GT_BARELY: return GT;
        default: return this;
        }
    }

    public String describe() {
        return description;
    }

    private ComparisonResult(String description) {
        this.description = description;
    }

    private final String description;
}
