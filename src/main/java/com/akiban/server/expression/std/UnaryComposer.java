
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;

import java.util.List;

abstract class UnaryComposer implements ExpressionComposer {

    protected abstract Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType);
    
    // For most expressions, NULL is contaminating
    // Any expressions that treat NULL specially should override this
    @Override
    public NullTreating getNullTreating()
    {
        return NullTreating.RETURN_NULL;
    }
        
    @Override
    public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
    {
        if (arguments.size() != 1)
            throw new WrongExpressionArityException(1, arguments.size());
        if (arguments.size() + 1 != typesList.size())
            throw new IllegalArgumentException("invalid argc");
        return compose(arguments.get(0), typesList.get(0), typesList.get(1));
    }
    
}
