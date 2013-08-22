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

import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;

public class PiExpression extends AbstractNoArgExpression
{
    @Scalar("pi")
    public static final ExpressionComposer COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return INSTANCE;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.DOUBLE;
        }
        
    };
    
    private static final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private static final ValueSource pi = new ValueHolder(AkType.DOUBLE, Math.PI);
        
        @Override
        public ValueSource eval()
        {
            return pi;
        }
    }

    @Override
    public String name()
    {
        return "PI";
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return EVALUATION;
    }
    
        
    protected PiExpression ()
    {
        super(AkType.DOUBLE);
    }
    
    private static final ExpressionEvaluation EVALUATION = new InnerEvaluation();
    private static final Expression INSTANCE = new PiExpression();
}
