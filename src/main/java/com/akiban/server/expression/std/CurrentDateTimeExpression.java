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
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import org.joda.time.DateTime;

public class CurrentDateTimeExpression extends AbstractNoArgExpression
{
    /**
     * return current_date() expression 
     */
    @Scalar({"current_date", "curdate"})
    public static final ExpressionComposer CURRENT_DATE_COMPOSER 
            = new DateTimeComposer(AkType.DATE);
    
    /**
     * return current_time() expression 
     */
    @Scalar({"current_time", "curtime"})
    public static final ExpressionComposer CURRENT_TIME_COMPOSER 
            = new DateTimeComposer(AkType.TIME);
    
    /**
     * return current_timestamp() expression in String
     * current_timestamp, now, localtime and localtimestamp all mean the same thimg
     */
    @Scalar({"current_timestamp", "now", "localtime", "localtimestamp"})
    public static final ExpressionComposer CURRENT_TIMESTAMP_COMPOSER 
            = new DateTimeComposer(AkType.DATETIME);


    private final AkType currentType;

    public CurrentDateTimeExpression(AkType currentType)
    {
        super(checkType(currentType));
        this.currentType = currentType;
    }

    private static final class InnerEvaluation extends AbstractNoArgExpressionEvaluation
    {
        private AkType currentType;
        private QueryContext context;

        public InnerEvaluation(AkType currentType)
        {
            this.currentType = currentType;
        }

        @Override
        public void of(QueryContext context) {
            this.context = context;
        }

        @Override
        public ValueSource eval()
        {
            valueHolder().putRaw(currentType, new DateTime(context.getCurrentDate()));
            return valueHolder();
        }
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean needsBindings() {
        return true;
    }

    private static final class DateTimeComposer extends NoArgComposer {
        private final AkType currentType;

        public DateTimeComposer(AkType currentType)
        {
            this.currentType = currentType;
        }
        
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(currentType);
        }

        @Override
        protected ExpressionType composeType()
        {
            return ExpressionTypes.newType(currentType, 0, 0);
        }        
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(currentType);
    }

    @Override
    public String name ()
    {
        return "CURRENT_" + currentType;
    }
    
    private static AkType checkType (AkType input)
    {
        if (input == AkType.DATE  || input == AkType.TIME || input == AkType.DATETIME) return input;
        else throw new UnsupportedOperationException("CURRENT_" + input + " is not supported");
    }
}
