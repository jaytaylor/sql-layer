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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CurrentDateTimeExpression extends AbstractVoidParamExpression
{
    /*
    @Scalar("current_date_s")
    public static final ExpressionComposer CURRENT_DATE_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true,AkType.DATE);
        }
    };

    @Scalar("current_date_n")
    public static final ExpressionComposer CURRENT_DATE_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.DATE);
        }
    };

    @Scalar("current_time_s")
    public static final ExpressionComposer CURRENT_TIME_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true, AkType.TIME);
        }
    };

    @Scalar("current_time_n")
    public static final ExpressionComposer CURRENT_TIME_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.TIME);
        }
    };

    @Scalar("current_timestamp_s")
    public static final ExpressionComposer CURRENT_TIMESTAMP_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true, AkType.TIMESTAMP);
        }
    };

    @Scalar ("now")
    public static final ExpressionComposer CURRENT_TIMESTAMP_ALIAS = CURRENT_TIMESTAMP_S_COMPOSER;
    
    @Scalar("current_timestamp_n")
    public static final ExpressionComposer CURRENT_TIMESTAMP_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.TIMESTAMP);
        }
    };
*/
    private final boolean isString;
    private final AkType neededInfo;

    public CurrentDateTimeExpression (boolean isString, AkType neededInfo)
    {
        super(isString? AkType.VARCHAR : AkType.LONG);
        this.isString = isString;
        this.neededInfo = neededInfo;
    }

    private static final class InnerEvaluation extends AbstractVoidParamExpressionEvaluation
    {
        private final boolean isString;
        private final AkType neededInfo;

        protected InnerEvaluation (boolean isString, AkType neededInfo)
        {
            this.isString = isString;
            this.neededInfo = neededInfo;
        }

        @Override
        public ValueSource eval()
        {
            if (isString)
                return new ValueHolder(AkType.VARCHAR, new SimpleDateFormat(getFormat()).format(new Date()));
            else
                return new ValueHolder(AkType.LONG, Long.parseLong(new SimpleDateFormat(getFormat()).format(new Date())));
        }

        private String getFormat ()
        {
            switch(neededInfo)
            {
                case DATE:      return isString ? "yyyy-MM-dd" : "yyyyMMdd";
                case TIME:      return isString ? "HH:mm:ss" : "HHmmss";
                case TIMESTAMP: return isString ? "yyyy-MM-dd HH:mm:ss" : "yyyyMMddHHmmss";
                default:        throw new UnsupportedOperationException("CURRENT_" + neededInfo + " is not supported");
            }
        }
    }

    @Override
    protected String name()
    {
        return "CURRENT_" + neededInfo;
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation (isString, neededInfo);
    }
}
