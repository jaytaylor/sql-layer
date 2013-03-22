
package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.extract.Extractors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class NotExpressionTest extends ComposedExpressionTestBase {

    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.BOOL, true);

    @Test
    public void notTrue() {
        assertEquals(Boolean.FALSE, getNot(true));
    }

    @Test
    public void notFalse() {
        assertEquals(Boolean.TRUE, getNot(false));
    }

    @Test
    public void notNull() {
        assertEquals(null, getNot(null));
    }

    // ComposedExpressionTestBase interface

    @Override
    protected CompositionTestInfo getTestInfo() {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() {
        return NotExpression.COMPOSER;
    }

    // private

    private static Boolean getNot(Boolean in) {
        return Extractors.getBooleanExtractor().getBoolean(
                new NotExpression(LiteralExpression.forBool(in)).evaluation().eval(),
                null
        );
    }

    @Override
    protected boolean alreadyExc()
    {
       return false;
    }
}
