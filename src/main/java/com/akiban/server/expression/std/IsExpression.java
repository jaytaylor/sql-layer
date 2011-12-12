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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.BoolValueSource;

public class IsExpression extends AbstractUnaryExpression
{    
   @Scalar ("is true")
   public static final ExpressionComposer IS_TRUE = new InnerComposer(TriVal.TRUE, false);

   @Scalar ("is false")
   public static final ExpressionComposer IS_FALSE = new InnerComposer(TriVal.FALSE, false);

   @Scalar ("is unknown")
   public static final ExpressionComposer IS_UNKNOWN= new InnerComposer(TriVal.UNKNOWN, false);

   @Scalar ("is not true")
   public static final ExpressionComposer IS_NOT_TRUE = new InnerComposer(TriVal.TRUE, true);

   @Scalar ("is not false")
   public static final ExpressionComposer IS_NOT_FALSE = new InnerComposer(TriVal.FALSE, true);

   @Scalar ("is not unknown")
   public static final ExpressionComposer IS_NOT_UNKNOWN = new InnerComposer(TriVal.UNKNOWN, true);

   protected static enum TriVal
   {
        TRUE, FALSE, UNKNOWN
   }

   private static class InnerComposer extends UnaryComposer 
   {
       private final TriVal triVal;
       private final boolean negate;
       
       protected InnerComposer (TriVal triVal, boolean negate)
       {
           this.triVal = triVal;
           this.negate = negate;
       }

        @Override
        protected Expression compose(Expression argument)
        {
            return new IsExpression(argument, triVal, negate);
        }

        @Override
        protected AkType argumentType(AkType givenType)
        {
            return Converters.isConversionAllowed(givenType, AkType.BOOL)
                    ? AkType.BOOL : AkType.NULL;
        }

        @Override
        protected ExpressionType composeType(ExpressionType argumentType)
        {
            return ExpressionTypes.BOOL;
        }       
   }

    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final TriVal triVal;
        private final boolean negate;

        public InnerEvaluation (ExpressionEvaluation operandEval, TriVal triVal, boolean negate)
        {
            super(operandEval);
            this.triVal = triVal;
            this.negate = negate;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource source = operand();
            boolean eval;

            if (source.isNull())            
                eval = triVal == TriVal.UNKNOWN;            
            else
                switch (triVal)
                {
                    case TRUE:  eval = source.getBool(); break;
                    case FALSE: eval = !source.getBool(); break;
                    default:    eval = false; 
                }

            return BoolValueSource.of(negate? !eval : eval);
        }
    }

    private final TriVal triVal;
    private final boolean negate;
    
    protected IsExpression (Expression arg, TriVal triVal, boolean negate)
    {
        super(AkType.BOOL, arg);
        this.triVal = triVal;
        this.negate = negate;
    }

    @Override
    protected String name()
    {
        return "NOT" + (negate ? " NOT " : " ") + triVal;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(operandEvaluation(), triVal, negate);
    }
}
