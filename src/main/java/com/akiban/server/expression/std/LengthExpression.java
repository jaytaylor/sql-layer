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
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.server.types.util.ValueHolder;

public class LengthExpression  extends AbstractUnaryExpression
{
    @Scalar ("Length")
    public static final ExpressionComposer COMPOSER = new UnaryComposer ()
    {
        @Override
        protected Expression compose(Expression argument) 
        {
            return new LengthExpression(argument);
        }

        @Override
        protected AkType argumentType() {
            return AkType.VARCHAR;
        }

        @Override
        protected ExpressionType composeType(ExpressionType argumentType) {
            return ExpressionTypes.LONG;
        }
    };
        
    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation
    {
        public InnerEvaluation (ExpressionEvaluation ev)
        {
            super(ev);
        }

        @Override
        public ValueSource eval() 
        {
           ValueSource source = this.operand();
           if (source.isNull()) return NullValueSource.only();
           
           ObjectExtractor<String> sExtractor = Extractors.getStringExtractor();
           String st = sExtractor.getObject(source);
           
           return new ValueHolder(AkType.LONG, st.length());
        }        
    }
    
    public LengthExpression (Expression e)
    {
        super(AkType.LONG, e);
    }

    @Override
    protected String name() 
    {
        return "Length";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation(this.operandEvaluation());
    }    
}
