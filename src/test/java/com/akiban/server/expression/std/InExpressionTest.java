
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
        List<Parameterization> params = new ArrayList<>();

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
