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
        public Expression compose(List<? extends Expression> arguments)
        {
            int size = arguments.size();
            if (size != 1 && size != 2)
                throw new WrongExpressionArityException(2, size);
 
            return new LogExpression(arguments, "LOG");
        }
        
        @Override
        public String toString ()
        {
            return "LOG";
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
        protected Expression compose(Expression argument)
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
    protected boolean nullIsContaminating()
    {
        return true;
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
