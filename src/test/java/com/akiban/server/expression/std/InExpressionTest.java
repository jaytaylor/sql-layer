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
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.akiban.server.expression.std.ExprUtil.exploding;
import static org.junit.Assert.assertEquals;
import static com.akiban.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public final class InExpressionTest {

    // test methods

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        List<Parameterization> params = new ArrayList<Parameterization>();

        addTo(params, lit(5), true, lit(3), lit(4), lit(5), exploding(AkType.VARCHAR));
        addTo(params, lit(5), false, lit(3));
        addTo(params, lit(3), true, lit("3"));
        addTo(params, lit(5), null, lit(3), constNull());
        addTo(params, lit(5), true, constNull(), lit(5));
        addTo(params, constNull(), null, exploding(AkType.VARCHAR));

        return params;
    }

    public InExpressionTest(Expression lhs, List<Expression> rhs, Boolean expected) {
        this.expected = expected;
        this.inExpression = new InExpression(lhs, rhs);
    }
    
    @Test
    public void test() {
        ValueSource answerSource = inExpression.evaluation().eval();
        Boolean answer = Extractors.getBooleanExtractor().getBoolean(answerSource, null);
        assertEquals(expected, answer);
    }

    // for use in this class

    private static void addTo(Collection<Parameterization> out, Expression lhs, Boolean result, Expression... rhs) {
        List<Expression> rhsList = Arrays.asList(rhs);
        Parameterization param = Parameterization.create(lhs + " IN " + rhsList, lhs, rhsList, result);
        out.add(param);
    }

    // object state
    
    private final Boolean expected;
    private final Expression inExpression;
}
