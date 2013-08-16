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

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidCharToNumException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueSources;
import com.foundationdb.sql.StandardException;
import java.util.Iterator;
import java.util.List;

public class FieldFunctionExpression extends AbstractCompositeExpression
{
    @Scalar("field")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() < 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
            
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            // don't really care about the types (for now)
            return new FieldFunctionExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
        
    };

    @Override
    public String name() {
        return "FIELD_FUNCTION";
    }
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> args)
        {
            super(args);
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource first = children().get(0).eval();
            long ret = 0;

            if (!first.isNull())
            {
            
                int n = 0;
                boolean homogeneous = true;
                Iterator<? extends ExpressionEvaluation> iter = children().iterator();
                iter.next();
                while (iter.hasNext())
                    if (!(homogeneous = first.getConversionType() == iter.next().eval().getConversionType()))
                        break;
                
                iter = children().iterator();
                iter.next();
                while (iter.hasNext())
                    try
                    {
                        ValueSource source = iter.next().eval();
                        ++n;
                        if (ValueSources.equals(source, first, !homogeneous))
                        {
                            ret = n;
                            break;
                        } 
                    }
                    catch (InvalidOperationException e)
                    {
                        QueryContext qc = queryContext();
                        if (qc != null)
                            qc.warnClient(e);
                    }
                    catch (NumberFormatException e) // when trying to compare 2 VARCHAR as double
                    {
                        QueryContext qc = queryContext();
                        if (qc != null)
                            qc.warnClient(new InvalidCharToNumException(e.getMessage()));
                    }
                
            }
            
            valueHolder().putLong(ret);
            return valueHolder();
        }
        
    }

    @Override
    public boolean nullIsContaminating()
    {
        return false;
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
    
    
    FieldFunctionExpression(List<? extends Expression> args)
    {
        super(AkType.LONG, checkArgs(args));
    }
    
    private static List<? extends Expression> checkArgs (List<? extends Expression> args)
    {
        if (args.size() < 2)
            throw new WrongExpressionArityException(2, args.size());
        return args;
    }
}
