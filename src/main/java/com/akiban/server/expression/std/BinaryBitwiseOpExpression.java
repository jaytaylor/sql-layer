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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;
import java.math.BigInteger;
import java.util.EnumSet;
import java.util.List;


public class BinaryBitwiseOpExpression extends AbstractBinaryExpression
{
    public static enum BitOperator
    {
        BITWISE_AND,
        BITWISE_OR,
        BITWISE_XOR,
        LEFT_SHIFT,
        RIGHT_SHIFT,
    }
    
    @Scalar("$")
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
    public static final EnumSet<AkType> SUPPORTED_TYPES = EnumSet.of(
            AkType.INT, AkType.LONG, AkType.U_BIGINT, AkType.U_INT);
    
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
            return new BinaryBitwiseOpExpression(first, op, second);
        }
    }   
    
    private static class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final BitOperator op;
        
        public InnerEvaluation (List<? extends ExpressionEvaluation> children, BitOperator op)
        {
            super(children);
            this.op = op;
        }

        @Override
        public ValueSource eval() 
        {
            ObjectExtractor<BigInteger> bIntExtractor = Extractors.getUBigIntExtractor();
            
            BigInteger left = bIntExtractor.getObject(left());
            BigInteger right = bIntExtractor.getObject(right());
            BigInteger top;
            
            switch(op)
            {
                case BITWISE_AND:           top = left.and(right); break;
                case BITWISE_OR:            top = left.or(right); break;
                case BITWISE_XOR:           top = left.xor(right); break;
                case LEFT_SHIFT:    top = left.shiftLeft(right.intValue()); break;
                default:            top = left.shiftRight(right.intValue()); break;
            }
      
            
            return new ValueHolder(AkType.U_BIGINT, top);
        }
        
    }
    
    public BinaryBitwiseOpExpression (Expression lhs, BitOperator op, Expression rhs)
    {
        super(AkType.U_BIGINT, lhs, rhs);
        this.op = op;
        checkType(lhs.valueType(), rhs.valueType());
    }
  

    private void checkType (AkType l, AkType r)
    {
            if (!SUPPORTED_TYPES.contains(r) || !SUPPORTED_TYPES.contains(l))
                throw new UnsupportedOperationException(l + " " + op + " " + r + " is not supported."); 
    }
    
    
    @Override
    protected void describe(StringBuilder sb) 
    {
        sb.append(op);
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(childrenEvaluations(), op);
    }
    
}
