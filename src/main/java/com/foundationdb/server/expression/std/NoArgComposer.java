/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
import com.foundationdb.sql.StandardException;
import com.foundationdb.server.expression.TypesList;

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
