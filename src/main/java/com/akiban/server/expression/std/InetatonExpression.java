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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;

public class InetatonExpression extends AbstractUnaryExpression
{   
    @Scalar ("inet_aton")
    public static final ExpressionComposer COMPOSER = new UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument)
        {
            return new InetatonExpression(argument);
        }
    };

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        public InnerEvaluation (ExpressionEvaluation ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval()
        {
            String ip = Extractors.getStringExtractor().getObject(operand());
            String str[];
            if (ip.contains("-") || (str = ip.split("\\.")).length > 4)
                return NullValueSource.only();
            try
            {
                short [] nums = new short[str.length];
                int i;
                long sum;
                for (i = 0; i < str.length; ++i)
                {
                    nums[i] = Short.parseShort(str[i]);
                    if (nums[i] > 255) return NullValueSource.only();
                }
                sum = nums[nums.length-1];
                if (!ip.contains(".")) return new ValueHolder(AkType.LONG,sum);
                for (i = 0; i < nums.length - 1; ++i)
                    sum += nums[i] * Math.pow(256, 3 - i);
                return new ValueHolder(AkType.LONG, sum);
            }
            catch (NumberFormatException e)
            {
                return NullValueSource.only();
            }
        }
    }

    public InetatonExpression (Expression e)
    {
        super(AkType.LONG, e);
    }

    @Override
    protected String name()
    {
        return "INET_ATON";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation());
    }
}
