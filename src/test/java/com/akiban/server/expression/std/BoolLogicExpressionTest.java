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
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicExpressionTest {

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        // OR logic
        pb.add("||", BoolLogicExpression.orComposer, true, ERR, true);

        pb.add("||", BoolLogicExpression.orComposer, false, TRUE, true);
        pb.add("||", BoolLogicExpression.orComposer, false, FALSE, false);
        pb.add("||", BoolLogicExpression.orComposer, false, NULL, null);

        pb.add("||", BoolLogicExpression.orComposer, null, TRUE, true);
        pb.add("||", BoolLogicExpression.orComposer, null, FALSE, null);
        pb.add("||", BoolLogicExpression.orComposer, null, NULL, null);

        // AND logic
        pb.add("&&", BoolLogicExpression.andComposer, true, TRUE, true);
        pb.add("&&", BoolLogicExpression.andComposer, true, FALSE, false);
        pb.add("&&", BoolLogicExpression.andComposer, true, NULL, null);

        pb.add("&&", BoolLogicExpression.andComposer, false, ERR, false);

        pb.add("&&", BoolLogicExpression.andComposer, null, TRUE, null);
        pb.add("&&", BoolLogicExpression.andComposer, null, FALSE, false);
        pb.add("&&", BoolLogicExpression.andComposer, null, NULL, null);

        for (Parameterization param : pb.asList()) {
            Boolean a = (Boolean)param.getArgsAsList().get(1);
            Expression b = (Expression)param.getArgsAsList().get(2);
            Boolean r = (Boolean)param.getArgsAsList().get(3);
            param.setName(String.format("%s %s %s -> %s", name(a), param.getName(), name(b), name(r)));
        }
        return pb.asList();
    }

    private static String name(Boolean b) {
        if (b == null)
            return "?";
        return b ? "T" : "F";
    }

    private static String name(Expression b) {
        if (b == TRUE)
            return "T";
        if (b == FALSE)
            return "F";
        if (b == NULL)
            return "?";
        if (b == ERR)
            return "x";
        throw new RuntimeException("unknown expression: " + b);
    }

    @Test
    public void test() {
        Expression test = composer.compose(Arrays.asList(lhs, rhs), Collections.nCopies(3, ExpressionTypes.BOOL));
        Boolean actual = Extractors.getBooleanExtractor().getBoolean(test.evaluation().eval(), null);
        assertEquals(expected, actual);
    }

    public BoolLogicExpressionTest(ExpressionComposer composer, Boolean lhs, Expression rhs, Boolean expected) {
        this.composer = composer;
        this.lhs = LiteralExpression.forBool(lhs);
        this.rhs = rhs;
        this.expected = expected;
    }

    private final ExpressionComposer composer;
    private final Expression lhs;
    private final Expression rhs;
    private final Boolean expected;

    private static final Expression TRUE = LiteralExpression.forBool(true);
    private static final Expression FALSE = LiteralExpression.forBool(false);
    private static final Expression NULL = LiteralExpression.forBool(null);
    private static final Expression ERR = ExprUtil.exploding(AkType.BOOL);
}
