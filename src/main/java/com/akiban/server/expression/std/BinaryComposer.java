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
import com.akiban.server.types.AkType;

import java.util.List;

abstract class BinaryComposer implements ExpressionComposer {

    protected abstract Expression compose(Expression first, Expression second);

    protected abstract ExpressionType composeType(ExpressionType first, ExpressionType second);

    @Override
    public Expression compose(List<? extends Expression> arguments) {
        if (arguments.size() != 2)
            throw new WrongExpressionArityException(2, arguments.size());
        return compose(arguments.get(0), arguments.get(1));
    }

    @Override
    public ExpressionType composeType(List<? extends ExpressionType> arguments) {
        if (arguments.size() != 2)
            throw new WrongExpressionArityException(2, arguments.size());
        return composeType(arguments.get(0), arguments.get(1));
    }
}
