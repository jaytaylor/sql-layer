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
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LogExpression extends AbstractCompositeExpression
{   
    @Scalar("log2")
    public static final ExpressionComposer LOG2 = new InnerComposer (2);
    
    @Scalar("log10")
    public static final ExpressionComposer LOG10 = new InnerComposer(10);
    
    @Scalar("ln")
    public static final ExpressionComposer LN = new InnerComposer(Math.E);
    
    @Scalar("log")
    public static final ExpressionComposer LOG = new ExpressionComposer ()
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
            return new LogExpression(arguments, new CompositeEvaluation(arguments));
        }
        
    };
            
    private static class InnerComposer extends UnaryComposer
    {
        double base;
        
        public InnerComposer (double base)
        {
            this.base = base;
        }
        
        @Override
        protected Expression compose(Expression argument)
        {
            return new LogExpression(Arrays.asList(argument), new UnaryEvaluation(argument, base));
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1) 
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.DOUBLE);
            return ExpressionTypes.DOUBLE;
        }
        
    }

    private static class CompositeEvaluation extends AbstractCompositeExpressionEvaluation
    {
        protected static List<ExpressionEvaluation> getChildrenEvals (List<? extends Expression> args)
        {
            List<ExpressionEvaluation> rst = new ArrayList(args.size());
            for (Expression arg : args)
                rst.add(arg.evaluation());
            return rst;
        }
        
        public CompositeEvaluation (List<? extends Expression> childrenEvals)
        {
            super(getChildrenEvals(childrenEvals));
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
                if (arg2.isNull() || num1 == 1 || (num2 = arg2.getDouble()) <= 0) 
                    return NullValueSource.only();
                
                valueHolder().putDouble(Math.log(num2) / Math.log(num1));
            }
            else
                valueHolder().putDouble(Math.log(num1));
            
            return valueHolder();
        } 
        
        @Override
        public String toString()
        {
            return "LOG";
        }
    }

    private static class UnaryEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private double base;
        public UnaryEvaluation (Expression ex, double base)
        {
            super(ex.evaluation());
            this.base = base;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource arg = operand();
            double num;
            
            if (arg.isNull() || (num = arg.getDouble()) <= 0)
                return NullValueSource.only();
            
            valueHolder().putDouble(Math.log(num) / Math.log(base));
            return valueHolder();
        } 
        
        @Override
        public String toString()
        {
            return "LOG_" + base; 
        }
    }
    
    private ExpressionEvaluation eval;
    
    protected LogExpression (List<? extends Expression> children, ExpressionEvaluation eval)
    {
        super(AkType.DOUBLE, children);
        this.eval = eval;
    }
    
    @Override
    protected boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(eval.toString());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return eval;
    }
}
