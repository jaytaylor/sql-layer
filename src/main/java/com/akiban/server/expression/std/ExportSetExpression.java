/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;

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
                            argumentTypes.setType(0, AkType.LONG);
                            break;
                default:    throw new WrongExpressionArityException(3, argumentTypes.size());
            }
       
            return ExpressionTypes.newType(AkType.VARCHAR, 
                           64 + 63 * (  Math.max(   argumentTypes.get(2).getPrecision(), 
                                                    argumentTypes.get(1).getPrecision()) 
                                        + argumentTypes.size() > 3 ? 
                                                    argumentTypes.get(3).getPrecision() : 
                                                    1), 
                            0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            if (arguments.size() < 3 || arguments.size() > 5)
                throw new WrongExpressionArityException(3, arguments.size());
            return new ExportSetExpression(arguments);
        }
        
    };

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
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
            
            long num = children().get(0).eval().getLong() ;//& 0xffffffffffffffffL; // truncate bits
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
            char digits[] = Long.toBinaryString(num).toCharArray();
            
            // return value is needs to be in little-endian format
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
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("EXPORT_SET");
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
