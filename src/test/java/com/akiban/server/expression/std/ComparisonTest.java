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
