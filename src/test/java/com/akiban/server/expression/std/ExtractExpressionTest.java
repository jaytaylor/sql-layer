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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.akiban.server.expression.std;

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.Expression;
import java.util.Date;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author vnguyen
 */
public class ExtractExpressionTest extends ComposedExpressionTestBase
{
    private static final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.DATE, false);

    @Test
    public void getDateFromDate()
    {
        Expression arg = new LiteralExpression(AkType.DATE, 1234L);
        Expression top =  ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));

        assertEquals (1234L, top.evaluation().eval().getDate());
    }
    
    @Test
    public void getDateFromDateTime ()
    {
        Expression arg = new LiteralExpression(AkType.DATETIME, 20091208123010L);
        Expression top =  ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));

        assertEquals ("2009-12-08", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }

    @Test 
    public void getDateFromTimestamp ()
    {
        Expression arg = new LiteralExpression (AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-30 12:30:10"));
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));
        
        assertEquals("2009-12-30", Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }
    
    
    
    @Test
    public void getDateFromVarchar ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-12-30 12:30:10");
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));
        
        assertEquals ("2009-12-30" , Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    }
    
    @Test
    public void getDateFromBadlyFormattedString ()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "2009-30-30 12:30:10");
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));
        
        assertTrue (top.evaluation().eval().isNull());
    }
    
    @Test
    public void getDateFromLong ()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 20091230123045L);
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));
        
        assertEquals ("2009-12-30" , Extractors.getLongExtractor(AkType.DATE).asString(top.evaluation().eval().getDate()));
    
    }
    
    @Test 
    public void getDateFromDouble ()
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 2345.5);
        Expression top = ExtractExpression.DATE_COMPOSER.compose(Arrays.asList(arg));
        
        assertTrue (top.evaluation().eval().isNull()); 
    }
    
    @Test
    public void getDayFromDate ()
    {
        Expression arg = new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-12-30"));
        Expression top = ExtractExpression.DAY_COMPOSER.compose(Arrays.asList(arg));
        
        assertEquals(30, top.evaluation().eval().getLong()); 
        
    }
    
    @Test
    public void getDatyFromDateTime()
    {
        Expression top = getTopExp(ExtractExpression.DAY_COMPOSER, getDatetime());
        assertEquals(8L, top.evaluation().eval().getLong());
    }
            

    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return ExtractExpression.DATE_COMPOSER;
    }
    
    private Expression getDatetime ()
    {
        return new LiteralExpression(AkType.DATETIME, 20091208123010L);
    }
    
    private Expression getDate ()
    {
        return new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-12-30"));
    }
    
    private Expression getTime ()
    {
        return new LiteralExpression(AkType.DATE, Extractors.getLongExtractor(AkType.TIME).getLong("11:30:45"));
    }
    
    private Expression getTimestamp()
    {
        return new LiteralExpression (AkType.TIMESTAMP,
                Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2009-12-30 12:30:10"));
    }
    private Expression getTopExp (ExpressionComposer composer, Expression arg)
    {
        return composer.compose(Arrays.asList(arg));
    }
    
}
