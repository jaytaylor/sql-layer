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

import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.util.List;
import org.joda.time.DateTime;


public class SysDateExpression extends AbstractNoArgExpression
{
    @Scalar("sysdate")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer()
    {

        @Override
        public void argumentTypes(List<AkType> argumentTypes) 
        {
            if (!argumentTypes.isEmpty()) throw new WrongExpressionArityException(0, argumentTypes.size());
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes) 
        {
             if (!argumentTypes.isEmpty()) throw new WrongExpressionArityException(0, argumentTypes.size());
             else return ExpressionTypes.DATETIME;
        }

        @Override
        public Expression compose(List<? extends Expression> arguments) 
        {
            if (!arguments.isEmpty()) throw new WrongExpressionArityException(0, arguments.size());
            else return new SysDateExpression();
        }
        
    };
    
    private static class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        @Override
        public ValueSource eval() 
        {
            return new ValueHolder(AkType.DATETIME, new DateTime());
        }
        
    }
    
    public SysDateExpression ()
    {
        super(AkType.DATETIME);
    }
    
    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    protected String name() 
    {
        return "SYSDATE()";
    }

    @Override
    public ExpressionEvaluation evaluation() 
    {
        return new InnerEvaluation();
    }
    
}
