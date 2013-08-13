/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.sql.StandardException;
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
