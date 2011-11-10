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
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CurrentDateTimeExpression extends AbstractVoidParamExpression
{
    /**
     * return current_date() expression in String
     */
    @Scalar("current_date_s")
    public static final ExpressionComposer CURRENT_DATE_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true,AkType.DATE, Context.NOW);
        }
    };
    
    /**
     * return current_date() expression in Long
     */
    @Scalar("current_date_n")
    public static final ExpressionComposer CURRENT_DATE_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.DATE, Context.NOW);
        }
    };

    /**
     * return current_time() expression in String
     */
    @Scalar("current_time_s")
    public static final ExpressionComposer CURRENT_TIME_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true, AkType.TIME, Context.NOW);
        }

        @Override
        public void argumentTypes(List<AkType> argumentTypes) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    };
    
    /**
     *  return current_time() expression in Long
     */
    @Scalar("current_time_n")
    public static final ExpressionComposer CURRENT_TIME_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.TIME, Context.NOW);
        }
    };

    /**
     * return current_timestamp() expression in String
     */
    @Scalar("current_timestamp_s")
    public static final ExpressionComposer CURRENT_TIMESTAMP_S_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(true, AkType.TIMESTAMP, Context.NOW);
        }
    };

    /**
     * return now() epxression  (an alias of current_timestamp()) in string
     */
    @Scalar ("now")
    public static final ExpressionComposer CURRENT_TIMESTAMP_ALIAS = CURRENT_TIMESTAMP_S_COMPOSER;
    
    /**
     * return current_timestamp() epxression in Long
     */
    @Scalar("current_timestamp_n")
    public static final ExpressionComposer CURRENT_TIMESTAMP_N_COMPOSER = new VoidComposer ()
    {
        @Override
        protected Expression compose()
        {
            return new CurrentDateTimeExpression(false, AkType.TIMESTAMP, Context.NOW);
        }
    };

    
    public static enum Context
    {
        NOW // need a better name!
        {
            @Override
            public Date getCurrent() { return new Date(); }
        }
        
        // TODO : add more "contexts", ie., decide what date() to return
        // as opposed to just returning new Date()
        ;
        
        protected abstract Date getCurrent();
    }

    private final boolean isString;
    private final AkType neededInfo;
    private final Context context;

    public CurrentDateTimeExpression (boolean isString, AkType neededInfo, Context context)
    {
        super(isString? AkType.VARCHAR : AkType.LONG);
        this.isString = isString;
        this.neededInfo = neededInfo;
        this.context = context;
    }

    private static final class InnerEvaluation extends AbstractVoidParamExpressionEvaluation
    {
        private final boolean isString;
        private final AkType neededInfo;
        private final Context context;

        protected InnerEvaluation (boolean isString, AkType neededInfo, Context context)
        {
            this.isString = isString;
            this.neededInfo = neededInfo;
            this.context = context;
        }

        @Override
        public ValueSource eval()
        {   
            if (isString)
                return new ValueHolder(AkType.VARCHAR, new SimpleDateFormat(getFormat()).format(context.getCurrent()));           
            else
                return new ValueHolder(AkType.LONG, Long.parseLong(new SimpleDateFormat(getFormat()).format(context.getCurrent())));

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
        return new InnerEvaluation (isString, neededInfo, context);
    }
}
