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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import java.util.List;

public abstract class AbstractThreeArgExpressionEvaluation extends AbstractCompositeExpressionEvaluation
{
    protected AbstractThreeArgExpressionEvaluation(List<? extends ExpressionEvaluation> children)
    {
        super(children);
        if (children().size() != 3)
        {
            throw new WrongExpressionArityException(3, children().size());
        }
    }

    protected ExpressionEvaluation firstEvaluation()
    {
        return children().get(0);
    }

    protected final ValueSource first() {
        return children().get(0).eval();
    }

    protected ExpressionEvaluation secondEvaluation()
    {
        return children().get(1);
    }

    protected final ValueSource second()
    {
        return children().get(1).eval();
    }

    protected ExpressionEvaluation thirdEvaluation()
    {
        return children().get(2);
    }

    protected final ValueSource third()
    {
        return children().get(2).eval();
    }

    protected final ValueSource[] getAll()
    {
        return new ValueSource[] {first(), second(), third()};
    }
}
