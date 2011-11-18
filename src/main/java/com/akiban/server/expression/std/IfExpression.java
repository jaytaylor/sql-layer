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
import java.math.BigInteger;
import java.util.EnumSet;
import java.util.List;

public class IfExpression extends AbstractCompositeExpression
{
    @Scalar("if")
    public static final ExpressionComposer COMPOSER = new ExpressionComposer ()
    {
        @Override
        public void argumentTypes(List<AkType> argumentTypes)
        {
           
        }

        @Override
        public ExpressionType composeType(List<? extends ExpressionType> argumentTypes)
        {
            int size = argumentTypes.size();
            if ( size != 3)  throw new WrongExpressionArityException(3, size);
            else return ExpressionTypes.newType(getTopType(argumentTypes.get(1).getType(), argumentTypes.get(2).getType()), 0, 0);
        }

        @Override
        public Expression compose(List<? extends Expression> arguments)
        {
            return new IfExpression(arguments);
        }
    };

    protected static final EnumSet<AkType> STRING = EnumSet.of(AkType.VARCHAR, AkType.TEXT);

    protected static AkType checkArgs (List<? extends Expression> children)
    {
        if (children.size() != 3) throw new WrongExpressionArityException(3, children.size());
        else return getTopType(children.get(1).valueType(), children.get(2).valueType());
    }

    static protected AkType getTopType (AkType o1, AkType o2)
    {
        if (o1 == o2) return o1;
        else if (!Converters.isConversionAllowed(o2, o2)) throw new UnsupportedOperationException("Inconvertible types " + o1 + " <=> " + o2);

        UnderlyingType under_o1 = o1.underlyingTypeOrNull();
        UnderlyingType under_o2 = o2.underlyingTypeOrNull();

        if (STRING.contains(o1) || STRING.contains(o2)) return AkType.VARCHAR;
        else if (o1 == AkType.DECIMAL || o2 == AkType.DECIMAL) return AkType.DECIMAL;
        else if (under_o1 == UnderlyingType.DOUBLE_AKTYPE || under_o2 == UnderlyingType.DOUBLE_AKTYPE) return AkType.DOUBLE;
        else if (under_o1 == UnderlyingType.FLOAT_AKTYPE || under_o2 == UnderlyingType.FLOAT_AKTYPE) return AkType.FLOAT;
        else if (o1 == AkType.U_BIGINT || o2 == AkType.U_BIGINT) return AkType.U_BIGINT;
        else if (under_o1 == UnderlyingType.LONG_AKTYPE || under_o2 == UnderlyingType.LONG_AKTYPE) return AkType.LONG;
        else return AkType.NULL;
    }

    private static class InnerEvaluation extends AbstractCompositeExpressionEvaluation
    {
        private AkType topType;
        private IfExpression exp;
        public InnerEvaluation ( IfExpression ex)
        {
            super(ex.childrenEvaluations());
            topType = ex.valueType();
            exp = ex;
        }

        @Override
        public ValueSource eval() 
        {
            ValueSource condition = this.children().get(0).eval();
            AkType condType = condition.getConversionType();
            int i ;
            
            if ( condition.isNull()) 
                i = 2;
            else
                switch (condType)
                {
                    case BOOL:      i = condition.getBool() ? 1 : 2; break;
                    case DATETIME:  
                    case DATE:
                    case TIME:
                    case INT:
                    case U_INT:                    
                    case TIMESTAMP:
                    case YEAR:
                    case LONG:      i = Extractors.getLongExtractor(condType).getLong(condition) != 0 ? 1 : 2; break;
                    case FLOAT:
                    case DOUBLE:
                    case U_FLOAT:
                    case U_DOUBLE:  i = Extractors.getDoubleExtractor().getDouble(condition) != 0.0 ? 1 : 2; break;
                    case DECIMAL:   i = condition.getDecimal().equals(BigDecimal.ZERO) ? 2 : 1; break;
                    case U_BIGINT:  i = condition.getUBigInt().equals(BigInteger.ZERO) ? 2 : 1; break;
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
                                    break;                   
                    default:        i = 2;

                    //TODO: case VARBINARY:
                }            
            return new CastExpression (topType, exp.children().get(i)).evaluation().eval();
        }        
    }
    
    public IfExpression (List <? extends Expression> children)
    {
        super(checkArgs(children), children);
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
       return new InnerEvaluation(this);
    }
}
