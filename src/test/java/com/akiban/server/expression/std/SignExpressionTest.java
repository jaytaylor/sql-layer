/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import static org.junit.Assert.*;
import org.junit.Test;

public class SignExpressionTest extends ComposedExpressionTestBase {
    
    private static final int POS = SignExpression.POS;
    private static final int NEG = SignExpression.NEG;
    private static final int ZERO = SignExpression.ZERO;

    @Test
    public void checkSignEnumValues()
    {
        // Ensure that we have -1 for negative, 0 for zero, 1 for positive
        assertEquals(1, POS);
        assertEquals(0, ZERO);
        assertEquals(-1, NEG);
    }
    
    @Test
    public void testDouble()
    {
        AkType testType = AkType.DOUBLE;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 123.456));
        Expression negOutput = new SignExpression(new LiteralExpression(testType, -2.2354));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0.0));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(NEG, negOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());
    }
    
    @Test
    public void testFloat()
    {
        AkType testType = AkType.FLOAT;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 123.456f));
        Expression negOutput = new SignExpression(new LiteralExpression(testType, -2.2354f));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0.0f));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(NEG, negOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());        
    }

    
    @Test
    public void testLong()
    {
        AkType testType = AkType.LONG;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 123456L));
        Expression negOutput = new SignExpression(new LiteralExpression(testType, -654321L));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(NEG, negOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());      
    }
        
    @Test
    public void testInt()
    {
        AkType testType = AkType.INT;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 25));
        Expression negOutput = new SignExpression(new LiteralExpression(testType, -10));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(NEG, negOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());      
    }
    
    @Test
    public void testDecimal()
    {
        AkType testType = AkType.DECIMAL;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, new BigDecimal("1234567.8987654321")));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, BigDecimal.ZERO));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt()); 
    }    
    
    // No tests for negative value since these values are unsigned
    @Test
    public void testBignum()
    {
        AkType testType = AkType.U_BIGINT;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, new BigInteger("1234567890987654321")));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, BigInteger.ZERO));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());          
    }
    
    @Test
    public void testUnsignedInt()
    {
        AkType testType = AkType.U_INT;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 5000));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt());    
    }
    
    @Test
    public void testUnsignedFloat()
    {
        AkType testType = AkType.U_FLOAT;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 1234.5678f));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0.0f));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt()); 
    }
        
    @Test
    public void testUnsignedDouble()
    {
        AkType testType = AkType.U_DOUBLE;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, 1234.5678d));
        Expression zeroOutput = new SignExpression(new LiteralExpression(testType, 0.0d));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(ZERO, zeroOutput.evaluation().eval().getInt()); 
    }
    
    @Test
    public void testNull()
    {
       Expression output = new SignExpression(new LiteralExpression(AkType.NULL, null));     
       ValueSource shouldBeNullValueSource = output.evaluation().eval();
       assertTrue("ValueSource of NULL input should be null", shouldBeNullValueSource.isNull());
    }
    
    @Test
    public void testInfinity()
    {
        AkType testType = AkType.DOUBLE;
        Expression posOutput = new SignExpression(new LiteralExpression(testType, Double.POSITIVE_INFINITY));
        Expression negOutput = new SignExpression(new LiteralExpression(testType, Double.NEGATIVE_INFINITY));
        
        assertEquals(POS, posOutput.evaluation().eval().getInt());
        assertEquals(NEG, negOutput.evaluation().eval().getInt());   
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.INT, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return SignExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
