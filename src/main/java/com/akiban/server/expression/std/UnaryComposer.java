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

abstract class UnaryComposer implements ExpressionComposer {

    protected abstract Expression compose(Expression argument);

    protected abstract AkType argumentType();

    protected abstract ExpressionType composeType(ExpressionType argumentType);

    @Override
    public Expression compose(List<? extends Expression> arguments) {
        if (arguments.size() != 1)
            throw new WrongExpressionArityException(1, arguments.size());
        return compose(arguments.get(0));
    }

    @Override
    public AkType argumentType(int index) {
        if (index != 0)
            throw new WrongExpressionArityException(1, index + 1);
        return argumentType();
    }

    @Override
    public ExpressionType composeType(List<? extends ExpressionType> arguments) {
        if (arguments.size() != 1)
            throw new WrongExpressionArityException(1, arguments.size());
        return composeType(arguments.get(0));
    }
}
