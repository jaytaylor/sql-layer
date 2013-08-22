/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


package com.foundationdb.server.expression.std;

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.ObjectExtractor;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.sql.StandardException;

public class CaseConvertExpression extends AbstractUnaryExpression
{
    public static enum ConversionType
    {
        TOUPPER,
        TOLOWER 
    }
    
    private final ConversionType conversionType;
    
    @Scalar ({"lcase", "lower"})
    public static final ExpressionComposer TOLOWER_COMPOSER = new InternalComposer(ConversionType.TOLOWER);
    
    @Scalar ({"ucase", "upper"})
    public static final ExpressionComposer TOUPPER_COMPOSER = new InternalComposer(ConversionType.TOUPPER);
    
    private static final class InternalComposer extends UnaryComposer
    {
        private final ConversionType conversionType;

        public InternalComposer (ConversionType conversionType)
        {
            this.conversionType = conversionType;
        }
        
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            return new CaseConvertExpression(argument, conversionType);
        }         

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 1)
                throw new WrongExpressionArityException(1, argumentTypes.size());
            argumentTypes.setType(0, AkType.VARCHAR);
            return argumentTypes.get(0);
        }
    }
    
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private final ConversionType conversionType;
        
        public InnerEvaluation (ExpressionEvaluation ev, ConversionType conversionType)
        {
            super(ev);
            this.conversionType = conversionType;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            
            ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
            String st = sExtractor.getObject(operandSource);
            valueHolder().putString(conversionType == ConversionType.TOLOWER ? st.toLowerCase() : st.toUpperCase());
            return valueHolder();
        }         
    }
     
    /**
     * 
     * @param operand
     * @param type 
     */
    public CaseConvertExpression (Expression operand, ConversionType type)
    {
        super(AkType.VARCHAR, operand);
        this.conversionType = type;
    }

    @Override
    public String name() 
    {
        return conversionType.name();
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(this.operandEvaluation(), conversionType);
    }
}
