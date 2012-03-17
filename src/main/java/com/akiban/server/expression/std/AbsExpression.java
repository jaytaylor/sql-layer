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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.*;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import java.math.BigDecimal;
import java.math.BigInteger;

public class AbsExpression extends AbstractUnaryExpression 
{    
    @Scalar ({"absolute", "abs"})
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends UnaryComposer
    {
        @Override
        protected Expression compose(Expression argument)
        {
            return new AbsExpression(argument);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1) 
                throw new WrongExpressionArityException(1, argumentTypes.size());
            
            // We want to return the original type with ABS, but cast VARCHAR to DOUBLE
            ExpressionType argExpType = argumentTypes.get(0);
            AkType argAkType = argExpType.getType();
            if (argAkType == AkType.VARCHAR)
                argAkType = AkType.DOUBLE;
            
            argumentTypes.setType(0, argAkType);
            
            return ExpressionTypes.newType(argAkType, argExpType.getPrecision(), argExpType.getScale());
        }
    }
    
    private static class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        public InnerEvaluation(ExpressionEvaluation eval)
        {
            super(eval);
        }
        
        @Override
        public ValueSource eval()
        {
            if (operand().isNull())
                return NullValueSource.only();
            AkType operandType = operand().getConversionType();
            
            switch (operandType) {
                case DOUBLE:
                    valueHolder().putDouble( Math.abs(operand().getDouble()) ); 
                    break;   
                case FLOAT:
                    valueHolder().putFloat( Math.abs(operand().getFloat()) ); 
                    break;
                case LONG:
                    valueHolder().putLong( Math.abs(operand().getLong()) ); 
                    break;
                case INT:
                    valueHolder().putInt( Math.abs(operand().getInt()) ); 
                    break;
                case DECIMAL:
                    valueHolder().putDecimal( operand().getDecimal().abs()); 
                    break;
                case U_DOUBLE: 
                case U_BIGINT: 
                case U_FLOAT: 
                case U_INT:
                    // Unsigned values remain the same
                    valueHolder().copyFrom(operand()); 
                    break;
                default:
                    QueryContext context = queryContext();
                    if (context != null)
                        context.warnClient(new InvalidArgumentTypeException("ABS: " + operandType.name()));
                    return NullValueSource.only();
            }
            
            return valueHolder();
        }  
        
    }
    
    protected AbsExpression(Expression operand)
    {
        // ctor sets type and value
        super(operand.valueType(), operand);
    }
    
    @Override
    protected String name() 
    {
        return "ABS";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(this.operandEvaluation());
    }
    
}
