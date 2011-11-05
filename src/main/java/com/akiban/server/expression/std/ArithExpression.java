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
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.AbstractArithValueSource;
import com.akiban.util.ArgumentValidation;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

public class ArithExpression extends AbstractBinaryExpression
{
    private final ArithOp op;
    protected final AkType topT;
    static public final List<AkType> SUPPORTED_TYPES = // Order is IMPORTANT
            Arrays.asList(AkType.DECIMAL, AkType.DOUBLE, AkType.U_BIGINT, AkType.LONG);
    static private final EnumSet<AkType> DATETIME_TYPES = EnumSet.of(AkType.DATE, AkType.TIME, AkType.DATETIME, AkType.TIMESTAMP);

    public ArithExpression (Expression lhs, ArithOp op, Expression rhs)
    {
        super(getTopType(lhs.valueType(), rhs.valueType()),lhs, rhs);       
        
        this.op = op; 
        topT = super.valueType();
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(op, topT, this.childrenEvaluations());
    }

    private static AkType getTopType (AkType leftT, AkType rightT)
    {
        if (leftT == AkType.NULL || rightT == AkType.NULL)
            return AkType.NULL;
        
        if ( !supported(leftT) )
        {
            if (!supported(rightT) || DATETIME_TYPES.contains(leftT))
                    throw new UnsupportedOperationException("Unsupported Operation: " + leftT.name() + " and " + rightT.name());
            else return rightT;            
        }
        else if (!supported(rightT))
        {
            if (DATETIME_TYPES.contains(rightT))
                 throw new UnsupportedOperationException("Unsupported Operation: " + leftT.name() + " and " + rightT.name());
            else
                return leftT;
        }      
     
        AkType res = null;
        for (AkType pos : SUPPORTED_TYPES)
        {
            res = findType (leftT, rightT, pos);
            if (res != null) return res;
        }
  
       return res; // should never make it to here
    }

   private static AkType findType (AkType l, AkType r, AkType x) 
    {
         if (r == x || l == x)
         {
             return x;
         }
         else
             return null;
    }
    
    private static Boolean supported (AkType x)
    {
       return SUPPORTED_TYPES.contains(x);
    }
     
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        @Override
        public ValueSource eval() 
        {  
            this.valueSource.setOperands(left(), right());
            return (ValueSource)this.valueSource;
        }
        
        private InnerEvaluation (ArithOp op, AkType topT,
                List<? extends ExpressionEvaluation> children)
        {
            super(children);
            this.valueSource = new InnerValueSource(op, topT);
        }
        
        private final InnerValueSource valueSource;        
    }
    
   private static class InnerValueSource extends AbstractArithValueSource
   {
       private final ArithOp op;
       private ValueSource left;
       private ValueSource right;
       private AkType topT;
       public InnerValueSource (ArithOp op,  AkType topT )
       {
           this.op = op;
           this.topT = topT;
       }
       
       public void setOperands (ValueSource left, ValueSource right)
       {
           ArgumentValidation.notNull("Left", left);
           ArgumentValidation.notNull("Right", right);
           this.left = left;
           this.right = right;
       }
       
       @Override
       public AkType getConversionType () 
       {
          return topT; 
       }

        @Override
        protected long rawLong() 
        {       
            return op.evaluate(Extractors.getLongExtractor(left.getConversionType()).getLong(left),
                    Extractors.getLongExtractor(right.getConversionType()).getLong(right));
        }

        @Override
        protected double rawDouble()
        {
            return op.evaluate(Extractors.getDoubleExtractor().getDouble(left),
                    Extractors.getDoubleExtractor().getDouble(right));
        }  

        @Override
        protected BigInteger rawBigInteger() 
        {                   
           return op.evaluate(Extractors.getUBigIntExtractor().getObject(left),
                   Extractors.getUBigIntExtractor().getObject(right));
        }

        @Override
        protected BigDecimal rawDecimal() 
        {
            return op.evaluate(Extractors.getDecimalExtractor().getObject(left),
                    Extractors.getDecimalExtractor().getObject(right));
        }

        @Override
        public boolean isNull() 
        {
            return left.isNull() || right.isNull();
        }
   }
}
