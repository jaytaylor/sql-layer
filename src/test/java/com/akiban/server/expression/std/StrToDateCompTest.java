
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

public class StrToDateCompTest extends ComposedExpressionTestBase
{
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo (2, AkType.NULL, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return StrToDateExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc() 
    {
        return false;
    }

}
