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
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;


public class TrimExpression extends AbstractUnaryExpression
{
    public static enum TrimType { LEADING, TRAILING}
    
    private final TrimType trimType;
    
    @Scalar ("ltrim")
    public static final ExpressionComposer LTRIM_COMPOSER = new InternalComposer(TrimType.TRAILING);
    
    @Scalar ("rtrim")
    public static final ExpressionComposer RTRIM_COMPOSER = new InternalComposer(TrimType.TRAILING);
    
    @Scalar ("trim")
    public static final ExpressionComposer TRIM_COMPOSER = new InternalComposer(null);
    
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        final TrimType trimType;
        
        public InnerEvaluation (ExpressionEvaluation ev, TrimType trimType)
        {
            super(ev);
            this.trimType = trimType;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource operandSource = this.operand();
            if (operandSource.isNull()) return NullValueSource.only();
            
            ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
            String st = sExtractor.getObject(operandSource);
            
            if (trimType != TrimType.TRAILING)
                st = ltrim(st);
            if (trimType != TrimType.LEADING)
                st = rtrim(st);          
            
            return new ValueHolder (AkType.VARCHAR, st);            
        }
        
        private static String ltrim (String st)
        {
            for (int n = 0; n < st.length(); ++n)
                if (!Character.isWhitespace(st.charAt(n)))
                    return st.substring(n);
            return "";
        }
        
        private static String rtrim (String st)
        {
            for (int n = st.length() - 1; n >= 0; --n)
                if(!Character.isWhitespace(st.charAt(n)))
                    return st.substring(0, n+1);                   
                
            return "";
        }        
    }
    
    private static final class InternalComposer extends UnaryComposer
    {
        private final TrimType trimType;
        
        public InternalComposer (TrimType trimType)
        {
            this.trimType = trimType;
        }

        @Override
        protected Expression compose(Expression argument) 
        {
            return new TrimExpression (argument,trimType);
        }
    }
  
    
    /**
     * type specifies whether to trim trailing or leading
     *      if type = RTRIM => trailing
     *         type = LTRIM => leading
     *         anything else => both trailing and leading
     * @param operand
     * @param type 
     */
    public TrimExpression (Expression operand, TrimType type)
    {
        super(AkType.VARCHAR, operand);
        this.trimType = type;
    }
    
    @Override
    protected String name() 
    {       
        return "TRIM " + (trimType == null ? "" : trimType.name());
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(this.operandEvaluation(), trimType);
    }    
}
