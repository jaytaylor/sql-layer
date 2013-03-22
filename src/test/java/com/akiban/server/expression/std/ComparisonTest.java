
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
