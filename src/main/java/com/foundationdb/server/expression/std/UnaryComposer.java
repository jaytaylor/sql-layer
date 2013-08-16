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
