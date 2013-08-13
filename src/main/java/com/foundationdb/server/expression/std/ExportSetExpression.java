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
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.sql.StandardException;
import java.util.List;
import java.math.BigInteger;

public class ExportSetExpression extends AbstractCompositeExpression
{
    @Scalar("export_set")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 5:     argumentTypes.setType(4, AkType.LONG);     // fall thru
                case 4:     argumentTypes.setType(3, AkType.VARCHAR); // fall thru
                case 3:     argumentTypes.setType(2, AkType.VARCHAR);
                            argumentTypes.setType(1, AkType.VARCHAR);
                            argumentTypes.setType(0, AkType.U_BIGINT);
                            break;
                default:    throw new WrongExpressionArityException(3, argumentTypes.size());
            }
            
             return ExpressionTypes.newType(AkType.VARCHAR, 
                           64 + 63 * (  Math.max(   argumentTypes.get(2).getPrecision(), 
                                                    argumentTypes.get(1).getPrecision()) 
                                        + (argumentTypes.size() > 3 ? 
                                                    argumentTypes.get(3).getPrecision() : 
                                                    1)), 
                            0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            if (arguments.size() < 3 || arguments.size() > 5)
                throw new WrongExpressionArityException(3, arguments.size());
            return new ExportSetExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
    };

    @Override
    public String name() {
        return "EXPORT_SET";
    }

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private static final BigInteger MASK = new BigInteger("ffffffffffffffff", 16);
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
        }
        
        @Override
        public ValueSource eval()
        {
            for (ExpressionEvaluation child :children())
                if (child.eval().isNull())
                    return NullValueSource.only();

            BigInteger num = children().get(0).eval().getUBigInt().and(MASK);
            String bits[] = new String[]{children().get(2).eval().getString(),
                                         children().get(1).eval().getString()};
            String delim = ",";
            int len = 64; 
            
            switch(children().size())
            {
                case 5: len = Math.min((int)children().get(4).eval().getLong(), len); // fall thru
                case 4: delim = children().get(3).eval().getString();
            }
            
            StringBuilder builder = new StringBuilder();
            char digits[] = num.toString(2).toCharArray();
            
            // return value needs to be in little-endian format
            int count = 0;
            for (int n = digits.length - 1; n >= 0 && count < len; --n, ++count)
                builder.append(bits[digits[n]-'0']).append(delim);
            
            // fill the rest with 'off'
            for (; count < len; ++count)
                builder.append(bits[0]).append(delim);
            if (!delim.equals(""))
                builder.deleteCharAt(builder.length() -1);
            
            valueHolder().putString(builder.toString());
            return valueHolder();
        }
        
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations());
    }
    
    private ExportSetExpression (List<? extends Expression> operands)
    {
        super(AkType.VARCHAR, operands);
    }
}
