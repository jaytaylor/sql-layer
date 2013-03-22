
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.List;

public abstract class AbstractTernaryExpression extends AbstractCompositeExpression
{
    public AbstractTernaryExpression (AkType type, List<? extends Expression> args)
    {
        super(type, args);
        if (args.size() != 3)
            throw new WrongExpressionArityException(3, args.size());
    }
    
    public AbstractTernaryExpression (AkType type, Expression first, Expression second, Expression third)
    {
        super(type, first, second, third);
    }

    protected final Expression first()
    {
        return children().get(0);
    }

    protected final Expression second()
    {
        return children().get(1);
    }

    protected final Expression third()
    {
        return children().get(2);
    }
}
