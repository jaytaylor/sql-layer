/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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
import java.util.Arrays;
import java.util.List;

public class LogExpression extends AbstractCompositeExpression
{
    private static enum Base
    {
        LOG2(2),
        LOG10(10),
        LN(Math.E);

        protected final Expression base;
        private Base (double base)
        {
            this.base = new LiteralExpression(AkType.DOUBLE, base);
        }
    }
    
    @Scalar("log2")
    public static final ExpressionComposer LOG2 = new InnerComposer (Base.LOG2);
    
    @Scalar("log10")
    public static final ExpressionComposer LOG10 = new InnerComposer(Base.LOG10);
    
    @Scalar("ln")
    public static final ExpressionComposer LN = new InnerComposer(Base.LN);
    
    @Scalar("log")
    public static final ExpressionComposer LOG = new  ExpressionComposer ()
    {
        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
        
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            int size = argumentTypes.size();
            if (size != 1 && size != 2)
                throw new WrongExpressionArityException(2, size);
            
            for (int n = 0; n < size; ++n)
                argumentTypes.setType(n, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
        
        @Override
        public String toString ()
        {
            return "LOG";
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            int size = arguments.size();
            if (size != 1 && size != 2)
                throw new WrongExpressionArityException(2, size);
 
            return new LogExpression(arguments, "LOG");
        }
    };
    
    private static class InnerComposer extends UnaryComposer
    {
        private final Base base;
        public InnerComposer (Base base)
        {
            this.base = base;
        }
        
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType)
        {
            return new LogExpression(Arrays.asList(base.base, argument), base.name());
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1) 
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
        
        @Override
        public String toString ()
        {
            return base.name();
        }
    }

    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation
    {   
        public CompositeEvaluation (List<? extends ExpressionEvaluation> childrenEvals)
        {
            super(childrenEvals);
        }

        @Override
        public ValueSource eval()
        {
            ValueSource arg1 = children().get(0).eval();               
            double num1;
            if (arg1.isNull() || (num1 = arg1.getDouble()) <= 0) 
                return NullValueSource.only();
    
            if (children().size() == 2)
            {            
                double num2; 
                ValueSource arg2 = children().get(1).eval();
                if (arg2.isNull() || num1 == 1 || Double.isInfinite(num1) ||
                        Double.isNaN(num1) || (num2 = arg2.getDouble()) <= 0)
                    return NullValueSource.only();
                
                valueHolder().putDouble(Math.log(num2) / Math.log(num1));
            }
            else
                valueHolder().putDouble(Math.log(num1));
            
            return valueHolder();
        }         
    }
    
    private final String name;

    protected LogExpression (List<? extends Expression> children, String name)
    {
        super(AkType.DOUBLE, children);
        this.name = name;
    }
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }
    
    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name);
    }

    @Override
    public String name ()
    {
        return name;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new CompositeEvaluation(childrenEvaluations());
    }
}
