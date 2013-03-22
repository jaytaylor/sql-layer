
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;

import java.util.List;

abstract class NoArgComposer implements ExpressionComposer {

    protected abstract Expression compose();

    protected abstract ExpressionType composeType();

    protected Expression compose (ExpressionType type)
    {
        return compose();
    }
    
    @Override
    public NullTreating getNullTreating()
    {
        // NULL would be contaminating, if there were one.
        return NullTreating.RETURN_NULL;
    }
    
    @Override
    public Expression compose (List<? extends Expression> arguments, List<ExpressionType> typesList)
    {
        if (!arguments.isEmpty())
            throw new WrongExpressionArityException(0, arguments.size());
        if (typesList.size() != 1)
            throw new IllegalArgumentException("invalid argc");
        return compose(typesList.get(0));
    }

    @Override
    public ExpressionType composeType(TypesList argumentTypes) throws StandardException
    {
        if (argumentTypes.size() != 0)
            throw new WrongExpressionArityException(0, argumentTypes.size());
        return composeType();
    }
}
