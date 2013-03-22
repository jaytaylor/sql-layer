
package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.sql.StandardException;
import java.util.List;

public class IfNullExpression extends CoalesceExpression
{
    @Scalar ("ifnull")
    public static final ExpressionComposer IFNULL_COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());
            return CoalesceExpression.COMPOSER.composeType(argumentTypes);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new IfNullExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
    };
    
    public IfNullExpression (List< ? extends Expression> children)
    {
        super(checkArgs(children));
    }

    private static  List<? extends Expression> checkArgs (List< ? extends Expression> children)
    {
        if (children.size() != 2) throw new WrongExpressionArityException(2, children.size());
        else return children;
    }
}
