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
import java.util.List;

public abstract class TernaryComposer implements ExpressionComposer
{
    protected abstract Expression compose(Expression first, Expression second, Expression third);
//    protected abstract ExpressionType composeType(ExpressionType first, ExpressionType second, ExpressionType third);

    @Override
    public Expression compose(List<? extends Expression> arguments)
    {
        if (arguments.size() != 3)
            throw new WrongExpressionArityException(3, arguments.size());
        return compose(arguments.get(0), arguments.get(1), arguments.get(2));
    }

//    @Override
//    public ExpressionType composeType(List<? extends ExpressionType> arguments)
//    {
//        if (arguments.size() != 3)
//            throw new WrongExpressionArityException(3, arguments.size());
//        return composeType(arguments.get(0), arguments.get(1), arguments.get(2));
//    }
}
