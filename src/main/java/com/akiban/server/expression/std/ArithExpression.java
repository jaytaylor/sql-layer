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
import java.util.List;

public class ArithExpression extends AbstractBinaryExpression
{
    private final ArithOp op;
    protected final AkType topT;
    protected static final  List<AkType> SUPPORTED_TYPES = Arrays.asList( // order is IMPORTANT!!!
            AkType.INTERVAL, AkType.DECIMAL, AkType.DATE, AkType.DOUBLE,
            AkType.TIME, AkType.U_BIGINT, AkType.DATETIME, AkType.LONG, AkType.YEAR);

    public ArithExpression (Expression lhs, ArithOp op, Expression rhs)
    {
        super(getTopType(lhs.valueType(), rhs.valueType(), op),lhs, rhs);
        
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

    /**
     * Date/time types:
     *      DATE - DATE, TIME - TIME, YEAR - YEAR , DATETIME - DATETIME, => result is an INTERVAL
     *      DATE + INTERVAL => DATE, TIME + INTERVAL => TIME, ....etc
     *      INTERVAL + DATE => DATE, INTERVAL + TIME => TIME, ....etc
     *      DATE - INTERVAL => DATE, TIME - INTERVAL => TIME, ....etc
     *      INTERVAL + INTERVAL => INTERVAL
     *      INTERVAL - INTERVAL => INTERVAL
     *      INTERVAL * n (of anytypes :double, long ,...etc) => INTERVAL
     *      INTERVAL / n = INTERVAL
     *
     *  Regular types:
     *      Anything [+/*-]  DECIMAL => DECIMAL
     *      Anything (except DECIMAL) [+/*-] DOUBLE = > DOUBLE
     *      Anything (except DECIMAL and DOUBLE) [+/*-] U_BIGINT => U_BIGINT
     *      LONG [+/*-] LONG => LONG
     *
     * Anything else is unsupported
     *
     * @param leftT
     * @param rightT
     * @param op
     * @return topType
     * @author VyNguyen
     */
    private static AkType getTopType (AkType leftT, AkType rightT, ArithOp op)
    {
       if (leftT == AkType.NULL || rightT == AkType.NULL)  return AkType.NULL;
       String msg = leftT.name() + " " + op.opName() + " " + rightT.name();
       int l = SUPPORTED_TYPES.indexOf(leftT), r = SUPPORTED_TYPES.indexOf(rightT);
       int prod = l*r, sum = r + l;

       if (sum <= -1 ) throw new UnsupportedOperationException(msg); // both are NOT supported || interval and a NOT supported
       if (prod == 0) // at least one is interval
       {
           if (sum %2 == 0) // datetime and interval alone
               switch (op.opName())
               {
                  case '-': if (r != 0) throw new UnsupportedOperationException(msg); // fall thru;  check if second operandis NOT interval E.g inteval - date? => nonsense
                  case '+': return SUPPORTED_TYPES.get(sum); // return date/time or interval
                  default: throw new UnsupportedOperationException(msg);
               }
            else // number and interval: an interval can be multiply with || divide by a number
            {
               if (op.opName() == '/' && l == 0 || op.opName() == '*') return AkType.INTERVAL;
               else throw new UnsupportedOperationException(msg);
            }
        }
        else if (prod > 0) // both are supported
        {
            if (prod % 2 == 1) // odd => numeric values only
            {
                for (int n = 1; n <= 7; n +=2)
                    if (l == n || r == n)  return SUPPORTED_TYPES.get(n);
                return leftT; //should never make it to here
            }
            else // even => at least one is datetime
            {
                if (l == r && op.opName() == '-') return AkType.INTERVAL;
                else throw new UnsupportedOperationException("");
            }
        }
        else // left || right is not supported
        {
            if( sum %2 == 1) throw new UnsupportedOperationException(msg); // date/times and unsupported
            else return SUPPORTED_TYPES.get(sum+1);
        }   
    }

    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        @Override
        public ValueSource eval() 
        {  
            valueSource.setOperands(left(), right());
            return valueSource;
        }
        
        private InnerEvaluation (ArithOp op, AkType topT,
                List<? extends ExpressionEvaluation> children)
        {
            super(children);
            valueSource = new InnerValueSource(op, topT);
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
        protected long rawInterval ()
        {
            switch (SUPPORTED_TYPES.indexOf(left.getConversionType()) - SUPPORTED_TYPES.indexOf(right.getConversionType()))
            {
                case -1:
                case 1:     return rawDecimal().longValue();
                case -3:
                case 3:     return (long)rawDouble();
                case -5:
                case 5:     return rawBigInteger().longValue();
                default:    return rawLong();
            }
        }

        @Override
        public boolean isNull() 
        {
            return left.isNull() || right.isNull();
        }       
   }
}
