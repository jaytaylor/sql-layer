/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import java.io.UnsupportedEncodingException;
import org.slf4j.LoggerFactory;

/**
 * 
 * This implements the (byte) LENGTH function, which returns the number of byte in
 * the string argument. 
 * 
 * This differs from LengthExpression in that if a string st has 5 2-byte characters
 *      - LenthExpression would return 5
 *      - OctetLengthExpression would return 10
 * 
 */
public class OctetLengthExpression extends AbstractUnaryExpression
{
    @Scalar ("getOctetLength")
    public static final ExpressionComposer COMPOSER = new LengthExpression.InternalComposer()
    {       
        @Override
        protected Expression compose(Expression argument, ExpressionType argType, ExpressionType resultType) 
        {
            String charset = "UTF-8";
            if ((argType != null) &&
                (argType.getCharacterAttributes() != null) &&
                (argType.getCharacterAttributes().getCharacterSet() != null))
                charset = argType.getCharacterAttributes().getCharacterSet();
            return new OctetLengthExpression(charset, argument);
        }
    };
        
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        private String charset;

        public InnerEvaluation (String charset, ExpressionEvaluation ev)
        {
            super(ev);
            this.charset = charset;
        }

        @Override
        public ValueSource eval() 
        {
           ValueSource source = this.operand();
           if (source.isNull()) return NullValueSource.only();
           
           ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
           String st = sExtractor.getObject(source);
           
           try {
               valueHolder().putLong(st.getBytes(charset).length);
           }
           catch (UnsupportedEncodingException ex) {
               LoggerFactory.getLogger(OctetLengthExpression.class).error("Un-recognised charset", ex);
               return NullValueSource.only();
           }
           return valueHolder();
        }        
    }
    
    public OctetLengthExpression (String charset, Expression e)
    {
        super(AkType.LONG, e);
        this.charset = charset;
    }

    @Override
    public String name() 
    {
        return "LENGTH";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(charset, operandEvaluation());
    }    

    private final String charset;
}
