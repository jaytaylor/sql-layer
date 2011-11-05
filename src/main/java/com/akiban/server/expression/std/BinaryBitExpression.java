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

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import java.util.List;
import org.slf4j.LoggerFactory;

public class BinaryBitExpression extends AbstractBinaryExpression
{
    public static enum BitOperator
    {
        BITWISE_AND
        {
            @Override
            public BigInteger exc (BigInteger left, BigInteger right) { return left.and(right);}
        },
        BITWISE_OR
        {
            @Override
            public BigInteger exc (BigInteger left, BigInteger right) { return left.or(right);}
        },
        BITWISE_XOR
        {
            @Override
            public BigInteger exc (BigInteger left, BigInteger right) { return left.xor(right);}
        },
        LEFT_SHIFT
        {
            @Override
            public BigInteger exc (BigInteger left, BigInteger right) { return left.shiftLeft(right.intValue()); }
        },
        RIGHT_SHIFT
        {
            @Override
            public BigInteger exc (BigInteger left, BigInteger right) { return left.shiftRight(right.intValue()); }
        };

        protected abstract BigInteger exc (BigInteger left, BigInteger right);
    }
    
    @Scalar("&")
    public static final ExpressionComposer B_AND_COMPOSER = new InternalComposer(BitOperator.BITWISE_AND);
    
    @Scalar("|")
    public static final ExpressionComposer B_OR_COMPOSER = new InternalComposer(BitOperator.BITWISE_OR);
    
    @Scalar("^")
    public static final ExpressionComposer B_XOR_COMPOSER = new InternalComposer(BitOperator.BITWISE_XOR);
    
    @Scalar("<<")
    public static final ExpressionComposer LEFT_SHIFT_COMPOSER = new InternalComposer(BitOperator.LEFT_SHIFT);
    
    @Scalar(">>")
    public static final ExpressionComposer RIGHT_SHIFT_COMPOSER = new InternalComposer(BitOperator.RIGHT_SHIFT);
        
    private final BitOperator op;
       
    private static class InternalComposer extends BinaryComposer
    {
        private final BitOperator op;
        
        public InternalComposer (BitOperator op)
        {
            this.op = op;
        }      

        @Override
        protected Expression compose(Expression first, Expression second) 
        {
            return new BinaryBitExpression(first, op, second);
        }
    }   
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final BitOperator op;                
        private final boolean topIsNull;

        public InnerEvaluation (List<? extends ExpressionEvaluation> children, BitOperator op, boolean topIsNull)
        {
            super(children);
            this.op = op;
            this.topIsNull = topIsNull;
        }
 
        @Override
        public ValueSource eval() 
        {
            if (topIsNull) return NullValueSource.only();

            ObjectExtractor<BigInteger> bIntExtractor = Extractors.getUBigIntExtractor();
            BigInteger left = BigInteger.ZERO, right = BigInteger.ZERO;
            try
            {
                left = bIntExtractor.getObject(left());
                right = bIntExtractor.getObject(right());
            }
            catch (InconvertibleTypesException ex)
            {
                // if invalid types are supplied, 0 is assumed to be input
               LoggerFactory.getLogger(BinaryBitExpression.class).debug(ex.getShortMessage() + "assume 0 as input");
            }
            finally 
            {                
                BigInteger n64 = BigInteger.valueOf(Long.MAX_VALUE);
                n64 = n64.multiply(BigInteger.valueOf(2));
                n64 = n64.add(BigInteger.ONE);
                return new ValueHolder(AkType.U_BIGINT, op.exc(left, right).and(n64));
            }
        }
    }
    
    public BinaryBitExpression (Expression lhs, BitOperator op, Expression rhs)
    {
        super(lhs.valueType() == AkType.NULL || rhs.valueType() == AkType.NULL ? AkType.NULL : AkType.U_BIGINT
                , lhs, rhs);
        this.op = op;        
    } 
    
    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {    
        return new InnerEvaluation(childrenEvaluations(), op, valueType() == AkType.NULL);
    }    
}
