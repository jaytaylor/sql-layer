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
    
    private AkType inputType;
    
    @Scalar("ABSOLUTE")
    public static final ExpressionComposer COMPOSER = new InternalComposer();
    
    private static class InternalComposer extends UnaryComposer
    {
        public InternalComposer()
        {
            
        }
        
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
            
            if (operandType == AkType.DOUBLE)
                valueHolder().putDouble( Math.abs(operand().getDouble()) );      
            else if (operandType == AkType.FLOAT)
                valueHolder().putFloat( Math.abs(operand().getFloat()) );
            else if (operandType == AkType.LONG)
                valueHolder().putLong( Math.abs(operand().getLong()) );
            else if (operandType == AkType.INT)
                valueHolder().putInt( Math.abs(operand().getInt()) );
            else if (operandType == AkType.U_BIGINT) // TO DO - check if .abs() should be done anyway, though it's U_BIGINT
                valueHolder().putUBigInt( operand().getUBigInt() );
            else if (operandType == AkType.DECIMAL)
                valueHolder().putDecimal( operand().getDecimal().abs());
            else
                throw new InvalidArgumentTypeException("ABS: " + operandType.name());

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
