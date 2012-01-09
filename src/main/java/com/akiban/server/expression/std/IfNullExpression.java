/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

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
        public Expression compose(List<? extends Expression> arguments)
        {
            return new IfNullExpression(arguments);
        }
    };
    
    protected IfNullExpression (List< ? extends Expression> children)
    {
        super(checkArgs(children));
    }

    private static  List<? extends Expression> checkArgs (List< ? extends Expression> children)
    {
        if (children.size() != 2) throw new WrongExpressionArityException(2, children.size());
        else return children;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("IF_NULL");
    }
}
