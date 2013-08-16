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

import com.foundationdb.sql.Main;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.service.functions.Scalar;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;

public abstract class VersionExpression extends AbstractNoArgExpression
{
    @Scalar("version_full")
    public static final ExpressionComposer FULL_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return FULL_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(FULL_VERSION_SOURCE.getString().length());
        }
    };

    @Scalar("version")
    public static final ExpressionComposer SHORT_COMPOSER = new NoArgComposer()
    {
        @Override
        protected Expression compose()
        {
            return SHORT_VERSION;
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.varchar(SHORT_VERSION_SOURCE.getString().length());
        }
    };
    
    private VersionExpression()
    {
        super(AkType.VARCHAR);
    }

    @Override
    public String name()
    {
        return "version";
    }
    
    // static members

    private static final ValueSource FULL_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, Main.VERSION_STRING);
    private static final ValueSource SHORT_VERSION_SOURCE = new ValueHolder(AkType.VARCHAR, Main.SHORT_VERSION_STRING);
    
    private static final Expression FULL_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return FULL_EVAL;
        }
    };
    
    private static final Expression SHORT_VERSION = new VersionExpression()
    {
        @Override
        public ExpressionEvaluation evaluation()
        {
            return SHORT_EVAL;
        }
    };
    
    private static final ExpressionEvaluation FULL_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return FULL_VERSION_SOURCE;
        }
    };
    
    private static final ExpressionEvaluation SHORT_EVAL = new AbstractNoArgExpressionEvaluation()
    {
        @Override
        public ValueSource eval()
        {
            return SHORT_VERSION_SOURCE;
        }   
    };
}
