
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

public class IfExpressionCompositionTest extends ComposedExpressionTestBase
{
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.LONG, false);
    }

    @Override
    public ExpressionComposer getComposer()
    {
        return IfExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }
}
