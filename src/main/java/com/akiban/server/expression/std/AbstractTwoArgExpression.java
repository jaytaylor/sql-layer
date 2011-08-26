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
import com.akiban.server.types.AkType;

import java.util.List;

public abstract class AbstractTwoArgExpression extends AbstractCompositeExpression {

    protected Expression left() {
        return children().get(0);
    }

    protected Expression right() {
        return children().get(1);
    }

    protected AbstractTwoArgExpression(AkType type, List<? extends Expression> children) {
        super(type, children);
        if (children().size() != 2) {
            throw new WrongExpressionArityException(2, children().size());
        }
    }
}
