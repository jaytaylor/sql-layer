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
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.util.Arrays;
import java.util.List;

public class LocateExpression extends AbstractCompositeExpression
{
    @Scalar ("position")
    public static final ExpressionComposer POSITION_COMPOSER = new BinaryComposer ()
    {

        @Override
        protected Expression compose(Expression first, Expression second) 
        {
            return new LocateExpression(Arrays.asList(first, second));
        }

        @Override
        protected ExpressionType composeType(ExpressionType first, ExpressionType second) 
        {
            return ExpressionTypes.LONG;
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes) 
        {
            if (argumentTypes.size() != 2) throw new WrongExpressionArityException(2, argumentTypes.size());
            argumentTypes.set(0, AkType.VARCHAR);
            argumentTypes.set(0, AkType.VARCHAR);
        }
        
    };
    
    @Scalar ("locate")
    public static final ExpressionComposer LOCATE_COMPOSER = new ExpressionComposer ()
    {

        @Override
        public void argumentTypes(List<AkType> argumentTypes) 
        {
            int s = argumentTypes.size();
            if (s!= 2 && s != 3) throw new WrongExpressionArityException(2, s);
            argumentTypes.set(0, AkType.VARCHAR);
            argumentTypes.set(1, AkType.VARCHAR);
            if (s == 3) argumentTypes.set(2, AkType.LONG);
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes) 
        {
            if (argumentTypes.size()!= 2 && argumentTypes.size() != 3) throw new WrongExpressionArityException(2, argumentTypes.size());
            return ExpressionTypes.LONG;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments) 
        {
            return new LocateExpression(arguments);
        }
        
    };
    
    private static final class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        public InnerEvaluation (List<? extends ExpressionEvaluation> childrenEval)
        {
            super(childrenEval);
        }

        @Override
        public ValueSource eval() 
        {
            // str operand
            ValueSource substrOp = children().get(0).eval();
            if (substrOp.isNull()) return NullValueSource.only();
            
            // substr operand
            ValueSource strOp = children().get(1).eval();
            if (substrOp.isNull()) return NullValueSource.only();
            
            String str = strOp.getString();
            String substr = substrOp.getString();
           
            // optional pos operand
            long pos = 0;
            if (children().size() == 3)
            {
                ValueSource posOp = children().get(2).eval();
                if (posOp.isNull()) return NullValueSource.only();
                pos = posOp.getLong() -1;
                if (pos < 0 || pos > str.length()) return new ValueHolder(AkType.LONG, 0L);
            }

            valueHolder().putLong(1 + (long)str.indexOf(substr, (int)pos));
            return valueHolder();
        }
        
    }
    
    public LocateExpression (List<? extends Expression> children)
    {
        super(AkType.LONG, checkArgs(children));
    }

    private static List<? extends Expression> checkArgs (List <? extends Expression> children)
    {
        if (children.size() != 2 && children.size() != 3) throw new WrongExpressionArityException(2, children.size());
        return children;
    }
    
    @Override
    protected boolean nullIsContaminating() 
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append("LOCATE");
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations());
    }
}
