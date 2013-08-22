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

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InconvertibleTypesException;
import com.foundationdb.server.error.InvalidCharToNumException;
import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class CastExpression extends AbstractUnaryExpression
{
    public CastExpression(AkType type, Expression operand) {
        super(type, operand);
    }

    @Override
    public String name() {
        return "CAST_" + valueType();
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        CompoundExplainer ex = super.getExplainer(context);
        ex.addAttribute(Label.OUTPUT_TYPE, PrimitiveExplainer.getInstance(valueType().name()));
        return ex;
    }
    
    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(valueType(), operandEvaluation());
    }

    private static final class InnerEvaluation extends AbstractUnaryExpressionEvaluation {
        public InnerEvaluation(AkType type, ExpressionEvaluation ev) {
            super(ev);
            this.type = type;
        }

        @Override
        public ValueSource eval() {
            ValueSource operandSource = operand();
            if (operandSource.isNull())
                return NullValueSource.only();
            if (type.equals(operandSource.getConversionType()))
                return operandSource;
            valueHolder().expectType(type);
            try
            {
                return Converters.convert(operandSource, valueHolder());
            }
            catch (InvalidDateFormatException e)
            {
                return errorCase(e);
            }
            catch (InconvertibleTypesException e)
            {
                return errorCase(e);
            }
            catch (InvalidCharToNumException e)
            {
                return errorCase(e);
            }
        }
        
        private ValueSource errorCase (InvalidOperationException e)
        {
            QueryContext qc = queryContext();
            if (qc != null)
                qc.warnClient(e);

            switch(type)
            {
                case DECIMAL:   valueHolder().putDecimal(BigDecimal.ZERO); break;
                case U_BIGINT:  valueHolder().putUBigInt(BigInteger.ZERO); break;
                case LONG:
                case U_INT:
                case INT:        valueHolder().putRaw(type, 0L); break;
                case U_DOUBLE:
                case DOUBLE:     valueHolder().putRaw(type, 0.0d); break;
                case U_FLOAT:
                case FLOAT:      valueHolder().putRaw(type, 0.0f); break;
                case TIME:       valueHolder().putTime(0L);
                default:         return NullValueSource.only();

            }
            return valueHolder();
        }
        private final AkType type;        
    }
}
