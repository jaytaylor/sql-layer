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

package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.expression.std.Matchers.Index;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;

public class SubstringIndexExpression extends AbstractTernaryExpression
{
    @Scalar("substring_index")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 3)
                throw new WrongExpressionArityException(3, argumentTypes.size());
            
            argumentTypes.setType(0, AkType.VARCHAR);
            argumentTypes.setType(1, AkType.VARCHAR);
            argumentTypes.setType(2, AkType.LONG);
            return argumentTypes.get(0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new SubstringIndexExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
    };

    @Override
    public String name() {
        return "SUBSTRING_INDEX";
    }
    
    private static class InnerEvaluation extends AbstractThreeArgExpressionEvaluation
    {
        private Matcher matcher = null;        
        private String oldSubstr;
        
        InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource strSource;
            ValueSource substrSource;
            ValueSource countSource;
            
            if ((strSource = first()).isNull() 
                    || (substrSource = second()).isNull()
                    || (countSource = third()).isNull())
                return NullValueSource.only();
            
            String str = strSource.getString();
            String substr = substrSource.getString();
            int count = (int)countSource.getLong();
            boolean signed;
                        
            if (count == 0 || str.isEmpty() || substr.isEmpty())
            {
                valueHolder().putString("");
                return valueHolder();
            }
            else if (signed = count < 0)
            {
                count = -count;
                str = new StringBuilder(str).reverse().toString();
                substr = new StringBuilder(substr).reverse().toString();
            }

            // try to reuse compiled pattern if possible
            if (matcher == null || !substr.equals(oldSubstr))
            {
                oldSubstr = substr;
                matcher = new Index(substr);
            }
            
            int index = matcher.match(str, count);

            
            String ret = index < 0 // no match found
                    ? str 
                    : str.substring(0, index);
            if (signed)
                ret = new StringBuilder(ret).reverse().toString();
            valueHolder().putString(ret);
            return valueHolder();
        }
        
    }
    
    SubstringIndexExpression (List<? extends Expression> args)
    {
        super(AkType.VARCHAR, args);
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
