
package com.akiban.server.expression;

import com.akiban.server.t3expressions.T3RegistryServiceImpl;
import com.akiban.server.t3expressions.TCastResolver;
import com.akiban.server.types3.Types3Switch;
import org.junit.After;
import org.junit.Before;

public abstract class OldExpressionTestBase {
    private boolean types3switch;

    @Before
    public final void setTypes3Switch() {
        types3switch = Types3Switch.ON;
        Types3Switch.ON = false;
    }

    @After
    public final void restoreTypes3Switch() {
        Types3Switch.ON = types3switch;
    }

    protected static synchronized TCastResolver castResolver() {
        if (castResolver == null)
            castResolver = T3RegistryServiceImpl.createTCastResolver();
        return castResolver;
    }

    private static TCastResolver castResolver;
}
