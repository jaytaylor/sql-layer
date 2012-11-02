/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.types3.texpressions;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;

public final class TComparisonExpression extends TComparisonExpressionBase {

    @Override
    protected boolean doEvaluate(TInstance leftInstance, PValueSource leftSource,
                              TInstance rightInstance, PValueSource rightSource)
    {
        TClass tClass = leftInstance.typeClass();
        assert rightInstance.typeClass().compatibleForCompare(tClass)
                : "type class mismatch: " + leftInstance + " != " + rightInstance;
        final int cmpI;
        if (collator != null) {
            assert tClass.underlyingType() == PUnderlying.STRING : tClass + ": " + tClass.underlyingType();
            String leftString = leftSource.getString();
            String rightString = rightSource.getString();
            cmpI = collator.compare(leftString, rightString);
        }
        else {
            cmpI = TClass.compare(leftInstance, leftSource, rightInstance, rightSource);
        }
        final Comparison actualComparison;
        if (cmpI == 0)
            actualComparison = Comparison.EQ;
        else if (cmpI < 0)
            actualComparison = Comparison.LT;
        else
            actualComparison = Comparison.GT;

        final boolean result;
        switch (actualComparison) {
        case EQ:
            result = (comparison == Comparison.EQ) || (comparison == Comparison.LE) || (comparison == Comparison.GE);
            break;
        case GT:
            result = (comparison == Comparison.GT || comparison == Comparison.GE || comparison == Comparison.NE);
            break;
        case LT:
            result = (comparison == Comparison.LT || comparison == Comparison.LE || comparison == Comparison.NE);
            break;
        default:
            throw new AssertionError(actualComparison);
        }
        return result;
    }

    @Override
    protected String comparisonName() {
        return comparison.toString();
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right) {
        this(left, comparison, right, null);
    }

    public TComparisonExpression(TPreparedExpression left, Comparison comparison, TPreparedExpression right,
                                 AkCollator collator) {
        super(left, right);
        TClass oneClass = left.resultType().typeClass();
        TClass twoClass = right.resultType().typeClass();
        if (!oneClass.compatibleForCompare(twoClass))
            throw new IllegalArgumentException("can't compare expressions of different types: " + left + " != " + right);
        if (collator != null && oneClass.underlyingType() != PUnderlying.STRING) {
            throw new IllegalArgumentException("collator provided, but " + oneClass + " is not a string type");
        }
        this.comparison = comparison;
        this.collator = collator;
    }

    private final Comparison comparison;
    private final AkCollator collator;
}
