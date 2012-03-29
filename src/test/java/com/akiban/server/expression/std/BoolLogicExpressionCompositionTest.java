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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;

@RunWith(NamedParameterizedRunner.class)
public final class BoolLogicExpressionCompositionTest extends ComposedExpressionTestBase {

    private static boolean alreadyExc = false;
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        return Arrays.asList(
                Parameterization.create("AND", BoolLogicExpression.andComposer),
                Parameterization.create("OR", BoolLogicExpression.orComposer)
        );
    }

    @Test
    public void testdummy ()
    {
        alreadyExc = true;
    }
    @Override
    protected CompositionTestInfo getTestInfo (){
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() {
        return composer;
    }

    public BoolLogicExpressionCompositionTest(ExpressionComposer composer) {
        this.composer = composer;
    }

    private final ExpressionComposer composer;
    private final CompositionTestInfo testInfo = new CompositionTestInfo(2, AkType.BOOL, true);

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
