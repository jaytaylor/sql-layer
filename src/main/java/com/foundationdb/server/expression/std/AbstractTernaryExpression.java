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

package com.akiban.server.expression.std;

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import java.util.List;

public abstract class AbstractTernaryExpression extends AbstractCompositeExpression
{
    public AbstractTernaryExpression (AkType type, List<? extends Expression> args)
    {
        super(type, args);
        if (args.size() != 3)
            throw new WrongExpressionArityException(3, args.size());
    }
    
    public AbstractTernaryExpression (AkType type, Expression first, Expression second, Expression third)
    {
        super(type, first, second, third);
    }

    protected final Expression first()
    {
        return children().get(0);
    }

    protected final Expression second()
    {
        return children().get(1);
    }

    protected final Expression third()
    {
        return children().get(2);
    }
}
