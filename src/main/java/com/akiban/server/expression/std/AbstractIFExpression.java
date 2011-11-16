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

import com.akiban.server.expression.Expression;
import java.util.List;

abstract class AbstractIFExpression extends AbstractCompositeExpression
{
    protected final Expression getReturnExp()
    {
        return children().get(index);
    }

    protected AbstractIFExpression(int index, List<? extends Expression> children)
    {
        super(children.get(index).valueType(), children);
        this.index = index;
    }

    /**
     * store the index of the expression to be returned
     */
    protected int index;
}
