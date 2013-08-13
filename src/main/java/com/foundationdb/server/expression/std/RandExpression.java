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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.util.List;
import java.util.Random;

public class RandExpression extends AbstractCompositeExpression
{
    @Scalar({"random", "rand"})
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            switch(argumentTypes.size())
            {
                case 1:     argumentTypes.setType(0, AkType.LONG); // fall thru
                case 0:     break;
                default:    throw new WrongExpressionArityException(1, argumentTypes.size());
            }

            return ExpressionTypes.DOUBLE;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            return new RandExpression(arguments);
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.IGNORE;
        }
        
    };

    @Override
    public String name() {
        return "RAND";
    }
    
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private  Random random;
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> evals)
        {
            super(evals);
            random = null; 
        }
        
        @Override
        public ValueSource eval()
        {
            if (random == null)
                switch(children().size())
                {
                    case 0:     
                        random = new Random(); 
                        break;

                    case 1:     
                        ValueSource source = children().get(0).eval();
                        if (source.isNull())
                            random = new Random(0L);
                         else
                            random = new Random(source.getLong());
                         break;

                    default:    
                        throw new WrongExpressionArityException(1, children().size());
                }
            
            valueHolder().putDouble(random.nextDouble());
            return valueHolder();
        }
        
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return false;
    }

    @Override
    public boolean isConstant()
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
    
    protected RandExpression (List<? extends Expression> exps)
    {
        super(checkArgs(exps), exps);
    }
    
    private static AkType checkArgs(List<? extends Expression> args)
    {
        if (args.size() != 0 && args.size() != 1)
            throw new WrongExpressionArityException(1, args.size());
        return AkType.DOUBLE;
            
    }
}
