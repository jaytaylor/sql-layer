
package com.akiban.server.types3.texpressions;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;

public final class TComparisonExpression extends TComparisonExpressionBase {

    @Override
    protected int compare(TInstance leftInstance, PValueSource leftSource,
                          TInstance rightInstance, PValueSource rightSource)
    {
        TClass tClass = leftInstance.typeClass();
        assert rightInstance.typeClass().compatibleForCompare(tClass)
                : "type class mismatch: " + leftInstance + " != " + rightInstance;
        if (collator != null) {
            assert tClass.underlyingType() == PUnderlying.STRING : tClass + ": " + tClass.underlyingType();
            String leftString = leftSource.getString();
            String rightString = rightSource.getString();
            return collator.compare(leftString, rightString);
        }
        else {
            return TClass.compare(leftInstance, leftSource, rightInstance, rightSource);
        }
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this(left, comparison, right, null);
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right,
                                 AkCollator collator) {
        super(left, comparison, right);
        TClass oneClass = left.resultType().typeClass();
        TClass twoClass = right.resultType().typeClass();
        if (!oneClass.compatibleForCompare(twoClass))
            throw new IllegalArgumentException("can't compare expressions of different types: " + left + " != " + right);
        if (collator != null && oneClass.underlyingType() != PUnderlying.STRING) {
            throw new IllegalArgumentException("collator provided, but " + oneClass + " is not a string type");
        }
        this.collator = collator;
    }

    private final AkCollator collator;
}
