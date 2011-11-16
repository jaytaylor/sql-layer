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
import com.akiban.server.types.extract.Extractors;
import java.util.List;

public class IfExpression extends AbstractIFExpression
{
    @Scalar("if()")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer ()
    {
        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes)
        {
            throw new UnsupportedOperationException("Cannot decide until evaluation is done.");
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new IfExpression(arguments);
        }
    };
    public IfExpression (List <? extends Expression> children)
    {
        super(checkArgs(children), children);
    }

    protected static AkType getTopType (List<? extends Expression> children)
    {
        if (children.size() != 3) throw new WrongExpressionArityException(3, children.size());

        return AkType.LONG;
    }

    private static int checkArgs (List<? extends Expression> children)
    {
        if (children.size() != 3) throw new WrongExpressionArityException(3, children.size());

        Expression condition = children.get(0);
        AkType type = condition.valueType();
        if (condition.evaluation().eval().isNull() || type.underlyingTypeOrNull() == null ) return 2;

        switch (type)
        {
            case BOOL:      return condition.evaluation().eval().getBool() ? 1 : 2;
            case DATETIME:
            case DATE:
            case TIME:
            case INT:
            case U_INT:
            case U_BIGINT:
            case TIMESTAMP:
            case YEAR:
            case LONG:       return Extractors.getLongExtractor(type).getLong(condition.evaluation().eval()) != 0 ? 1 : 2;
            case FLOAT:
            case DOUBLE:
            case U_FLOAT:
            case U_DOUBLE:
            case DECIMAL:
                return Extractors.getDoubleExtractor().getDouble(condition.evaluation().eval()) != 0.0 ? 1 : 2;
            case VARCHAR:
            case TEXT:      String st = Extractors.getStringExtractor().getObject(condition.evaluation().eval());
                            long l;
                            try
                            {
                                l = Long.parseLong(st);
                            }
                            catch (NumberFormatException e)
                            {
                                return 1;
                            }
                            return l != 0 ? 1 : 2;
            default:
                return condition.evaluation().eval().getVarBinary().byteArrayLength() != 0 ? 1 : 2;
        }    
    }
 
    @Override
    protected boolean nullIsContaminating()
    {
        return false;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append("IF()");
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return getReturnExp().evaluation();
    }

}
