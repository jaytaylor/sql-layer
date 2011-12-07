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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import java.util.List;

public class DateFormatExpression extends AbstractBinaryExpression
{
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource date = children().get(0).eval();
            ValueSource format = children().get(1).eval();
            if (date.isNull() || format.isNull()) return NullValueSource.only();
            
            switch()
        }
        
    }
    
    protected DateFormatExpression (Expression left, Expression right)
    {
        super(AkType.VARCHAR, left, right);
    }
    
    @Override
    protected boolean nullIsContaminating() 
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append("DATE_FORMAT");
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluation());
    }
    
}
