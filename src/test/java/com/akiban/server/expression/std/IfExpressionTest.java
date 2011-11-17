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

import java.math.BigDecimal;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

import com.akiban.server.types.conversion.Converters;
import java.math.BigInteger;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class IfExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo testInfo = new CompositionTestInfo(3, AkType.LONG, false);

    static interface PsuedoExtractor
    {
        ValueHolder getValue (ValueSource source);
    }
    
    private static final PsuedoExtractor EXTRACTORS[] = new PsuedoExtractor []
    {
        new PsuedoExtractor() { public ValueHolder getValue (ValueSource source) { return new ValueHolder(source.getConversionType(), source.getDecimal());}},
        new PsuedoExtractor() { public ValueHolder getValue (ValueSource source) { return new ValueHolder(source.getConversionType(), source.getDouble());}},
        new PsuedoExtractor() { public ValueHolder getValue (ValueSource source) { return new ValueHolder(source.getConversionType(), source.getUBigInt());}},
        new PsuedoExtractor() { public ValueHolder getValue (ValueSource source) { return new ValueHolder(source.getConversionType(), source.getLong());}},
    };
    
    @Test
    public void test ()
    {
       Expression cond = new LiteralExpression(AkType.BOOL, false);
       Expression trueExp = new LiteralExpression(AkType.DECIMAL, BigDecimal.ONE);
       Expression falseExp = new LiteralExpression(AkType.DOUBLE, 2.3);
       Expression ifExp = new IfExpression(Arrays.asList(cond, trueExp, falseExp));
       
       assertTrue(ifExp.evaluation().eval().getDecimal().doubleValue() == 2.3);

    }

    @Test
    public void test2()
    {
        Expression cond = new LiteralExpression(AkType.DOUBLE, 2.3);
        
    }

    private Expression getCondExp (AkType type, boolean tf)
    {
        switch (type)
        {
            case BOOL:      return LiteralExpression.forBool(tf);
            case DECIMAL:   return new LiteralExpression(type, tf? BigDecimal.ONE: BigDecimal.ZERO);
            case U_BIGINT:  return new LiteralExpression(type, tf? BigInteger.ONE: BigInteger.ZERO);
            case DOUBLE:    return new LiteralExpression(type, tf? 1.0 : 0.0);
            case LONG:      return new LiteralExpression(type, tf? 1L : 0L);
            case NULL:      return LiteralExpression.forNull();
            case VARCHAR:
            case TEXT:      return new LiteralExpression(type, tf? "true" : "0");
            default:        return LiteralExpression.forNull();
        }
    }
    
        
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return testInfo;
    }

    @Override
    public ExpressionComposer getComposer()
    {
        return IfExpression.COMPOSER;
    }
   

}