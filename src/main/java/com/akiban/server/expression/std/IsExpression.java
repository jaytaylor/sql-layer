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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.BoolValueSource;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ArgList;

public class IsExpression extends AbstractUnaryExpression
{    
   @Scalar ("istrue")
   public static final ExpressionComposer IS_TRUE = new InnerComposer(TriVal.TRUE);

   @Scalar ("isfalse")
   public static final ExpressionComposer IS_FALSE = new InnerComposer(TriVal.FALSE);

   @Scalar ("isunknown")
   public static final ExpressionComposer IS_UNKNOWN= new InnerComposer(TriVal.UNKNOWN);


   protected static enum TriVal
   {
        TRUE, FALSE, UNKNOWN
   }

   private static class InnerComposer  extends UnaryComposer
   {
       protected final TriVal triVal;
       
       protected InnerComposer (TriVal triVal)
       {
           this.triVal = triVal;
       }

        @Override
        protected Expression compose(Expression argument)
        {
            return new IsExpression(argument, triVal);
        }

       @Override
       public String toString()
       {
           return "IS " + triVal;
       }

        @Override
        public ExpressionType composeType(ArgList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setArgType(0, AkType.BOOL);
            return ExpressionTypes.BOOL;
        }
    }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TriVal triVal;

        public InnerEvaluation (ExpressionEvaluation operandEval, TriVal triVal)
        {
            super(operandEval);
            this.triVal = triVal;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();


            if (source.isNull())            
                return BoolValueSource.of(triVal == TriVal.UNKNOWN);            
            else
                switch (triVal)
                {
                    case TRUE:  return BoolValueSource.of(source.getBool());
                    case FALSE: return BoolValueSource.of(!source.getBool()); 
                    default:    return BoolValueSource.of(false); 
                }
        }
    }

    private final TriVal triVal;
    
    protected IsExpression (Expression arg, TriVal triVal)
    {
        super(AkType.BOOL, arg);
        this.triVal = triVal;
    }

    @Override
    protected String name()
    {
        return "IS " + triVal;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), triVal);
    }
}
