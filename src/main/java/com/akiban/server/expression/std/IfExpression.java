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
import com.akiban.server.types.AkType.UnderlyingType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;

public class IfExpression extends AbstractCompositeExpression
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
        super(getTopType(children), children);
    }

    protected static final EnumSet<AkType> STRING = EnumSet.of(AkType.VARCHAR, AkType.TEXT);

    protected static AkType getTopType (List<? extends Expression> children)
    {
        if (children.size() != 3) throw new WrongExpressionArityException(3, children.size());

        AkType o1 = children.get(1).valueType();
        AkType o2 = children.get(2).valueType();
        
        if (o1 == o2) return o1;
        else if (!Converters.isConversionAllowed(o2, o2)) throw new UnsupportedOperationException("Inconvertible types " + o1 + " <=> " + o2);
        
        UnderlyingType under_o1 = o1.underlyingTypeOrNull();
        UnderlyingType under_o2 = o2.underlyingTypeOrNull();

        
        if (STRING.contains(o1) || STRING.contains(o2)) return AkType.VARCHAR;
        else if (o1 == AkType.DECIMAL || o2 == AkType.DECIMAL) return AkType.DECIMAL;
        else if (under_o1 == UnderlyingType.DOUBLE_AKTYPE || under_o2 == UnderlyingType.DOUBLE_AKTYPE) return AkType.DOUBLE;
        else if (under_o1 == UnderlyingType.FLOAT_AKTYPE || under_o2 == UnderlyingType.FLOAT_AKTYPE) return AkType.FLOAT;
        else if (under_o1 == UnderlyingType.LONG_AKTYPE || under_o2 == UnderlyingType.LONG_AKTYPE) return AkType.LONG;
        else return AkType.NULL;

    }

 
    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private AkType topType;
        private IfExpression exp;
        public InnerEvaluation (AkType type, IfExpression ex)
        {
            super(ex.childrenEvaluations());
            topType = type;
            exp = ex;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource condition = this.children().get(0).eval();
            AkType condType = condition.getConversionType();
            int i ;
            
            if ( condition.isNull()) 
                i = 1;
            else
                switch (condType)
                {
                    case BOOL:      i = condition.getBool() ? 1 : 2; break;
                    case DATETIME:  
                    case DATE:
                    case TIME:
                    case INT:
                    case U_INT:
                    case U_BIGINT:
                    case TIMESTAMP:
                    case YEAR:
                    case LONG:      i = Extractors.getLongExtractor(condType).getLong(condition) != 0 ? 1 : 2; break;
                    case FLOAT:
                    case DOUBLE:
                    case U_FLOAT:
                    case U_DOUBLE:  i = Extractors.getDoubleExtractor().getDouble(condition) != 0.0 ? 1 : 2; break;
                    case DECIMAL:   i = condition.getDecimal().equals(BigDecimal.ZERO) ? 2 : 1;
                    case VARCHAR:
                    case TEXT:      String st = Extractors.getStringExtractor().getObject(condition); 
                                    double l;
                                    try
                                    {
                                        l = Double.parseDouble(st);
                                        i = l != 0.0 ? 1 : 2;
                                    }
                                    catch (NumberFormatException e)
                                    {
                                        i = 1;
                                    }  
                    default:        i = 2;
                }
            
            CastExpression rst = new CastExpression (topType, exp.children().get(i));
            return rst.evaluation().eval();
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
       return new InnerEvaluation(this.valueType(), this);
    }

}
