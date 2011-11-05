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
import com.akiban.server.types.extract.LongExtractor;
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
            protected BigInteger exc (ValueSource left, ValueSource right)
            {
                return bIntExtractor.getObject(left).and(bIntExtractor.getObject(right));
            }

            /*
            @Override
            public BigInteger exc (BigInteger left, long right)
            {
                throw new UnsupportedOperationException("not supported yet. use exc(BigInt, BigInt");
            }
             *
             */
        },
        BITWISE_OR
        {
            @Override
            public BigInteger exc (ValueSource left, ValueSource right)
            {
                return bIntExtractor.getObject(left).or(bIntExtractor.getObject(right));
            }
        },
        BITWISE_XOR
        {
            @Override
            protected BigInteger exc (ValueSource left, ValueSource right)
            {
                return bIntExtractor.getObject(left).xor(bIntExtractor.getObject(right));
            }
        },
        LEFT_SHIFT
        {
            @Override
            protected BigInteger exc (ValueSource left, ValueSource right)
            {
                return bIntExtractor.getObject(left).shiftLeft((int)lExtractor.getLong(right));
            }
        },
        RIGHT_SHIFT
        {
            @Override
            protected BigInteger exc (ValueSource left, ValueSource right)
            {
                return bIntExtractor.getObject(left).shiftRight((int)lExtractor.getLong(right));
            }
        };

        protected abstract BigInteger exc (ValueSource left,ValueSource right);
        private static ObjectExtractor<BigInteger> bIntExtractor = Extractors.getUBigIntExtractor();
        private static LongExtractor lExtractor = Extractors.getLongExtractor(AkType.LONG);
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
       
    protected static class InternalComposer extends BinaryComposer
    {
        protected final BitOperator op;
        
        public InternalComposer (BitOperator op)
        {
            this.op = op;
        }      

        @Override
        protected Expression compose(Expression first, Expression second) 
        {
            return new BinaryBitExpression(first, op,second);
        }
    }

    protected static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
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

            BigInteger rst = BigInteger.ZERO;
            try
            {
                rst = op.exc(left(), right());
            }
            catch (InconvertibleTypesException ex)
            {
                // if invalid types are supplied, 0 is assumed to be input
               LoggerFactory.getLogger(BinaryBitExpression.class).debug(ex.getShortMessage() + " - assume 0 as input");
            }
            catch (Exception ex)
            {
                LoggerFactory.getLogger(BinaryBitExpression.class).debug(ex.getMessage() + "- assume 0 as input");
            }
            finally
            {
                BigInteger n64 = BigInteger.valueOf(Long.MAX_VALUE);
                n64 = n64.multiply(BigInteger.valueOf(2));
                n64 = n64.add(BigInteger.ONE);
                return new ValueHolder(AkType.U_BIGINT, rst.and(n64));
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
