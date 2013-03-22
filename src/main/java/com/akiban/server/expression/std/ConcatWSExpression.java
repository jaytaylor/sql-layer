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
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionComposer.NullTreating;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.Iterator;
import java.util.List;

public class ConcatWSExpression extends AbstractCompositeExpression
{
    @Scalar("concat_ws")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() < 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            
            int len = 0;
            int dLen = argumentTypes.get(0).getPrecision();
            
            for (int n = 1; n < argumentTypes.size(); ++n)
            {
                argumentTypes.setType(n, AkType.VARCHAR);
                len += argumentTypes.get(n).getPrecision() + dLen;
            }
            if (len > 0)
                len -= dLen; // delete the last delilmeter

            return ExpressionTypes.varchar(len);            
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new ConcatWSExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.REMOVE_AFTER_FIRST;
        }
    };
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation(List<? extends ExpressionEvaluation> args)
        {
            super(args);
        }

        @Override
        public ValueSource eval()
        {
            Iterator<? extends ExpressionEvaluation> iter = children().iterator();
            ValueSource delimeterSource = iter.next().eval();
            
            if (delimeterSource.isNull())
                return NullValueSource.only();
            
            String delimeter = delimeterSource.getString();
            StringBuilder bd = new StringBuilder();

            while (iter.hasNext())
            {
                ValueSource arg = iter.next().eval();
                if (!arg.isNull())
                    bd.append(arg.getString()).append(delimeter);
            }
            //remove the last delimeter
            if(bd.length() > 0)
                bd.delete(bd.length() - delimeter.length(),
                          bd.length());

            valueHolder().putString(bd.toString());
            return valueHolder();
        }
    }

    protected ConcatWSExpression(List<? extends Expression> args)
    {
        super(AkType.VARCHAR, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("CONCAT_WS");
    }

    @Override
    public boolean nullIsContaminating()
    {
        // NULL is comtaminating only when it's the first arg
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }

    @Override
    public String name()
    {
        return "CONCAT_WS";
    }
}
