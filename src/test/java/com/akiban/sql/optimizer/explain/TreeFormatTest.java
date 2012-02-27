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

package com.akiban.sql.optimizer.explain;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.std.ArithOps;
import com.akiban.server.expression.std.FromUnixExpression;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.expression.std.SubStringExpression;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.explain.std.TreeFormat;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class TreeFormatTest
{
    @Test
    public void testExplainerInExpression()
    {
        // parent expresion: 
        // SUBSTRING(FROM_UNIXTIME(123456 * 7 + 8, "%Y-%m-%d"), 9 + 10, 11)
        
        // literal(11)
        Expression lit_11 = new LiteralExpression(AkType.LONG, 11L);
        
        // literal(9)
        Expression lit_9 = new LiteralExpression(AkType.LONG, 9L);
        
        // literal(10)
        Expression lit_10 = new LiteralExpression(AkType.LONG, 10L);
        
        // literal(7)
        Expression lit_7 = new LiteralExpression(AkType.LONG, 7L);
        
        // literal 8
        Expression lit_8 = new LiteralExpression(AkType.LONG, 8L);
        
        // literal 123456
        Expression lit_123456 = new LiteralExpression(AkType.LONG, 123456L);
        
        // literal varchar exp
        Expression lit_varchar = new LiteralExpression(AkType.VARCHAR, "%Y-%m-%d");
        
        // from times exp
        Expression times = ((ExpressionComposer)ArithOps.MULTIPLY).compose(Arrays.asList(lit_123456, lit_7));
        Expression add = ((ExpressionComposer)ArithOps.ADD).compose(Arrays.asList(times, lit_8));
        
        Expression arg2 = ((ExpressionComposer)ArithOps.ADD).compose(Arrays.asList(lit_9, lit_10));
        
        // from unix exp
        Expression arg1 = FromUnixExpression.COMPOSER.compose(Arrays.asList(add, lit_varchar));
        
        // substr exp
        Expression substr = SubStringExpression.COMPOSER.compose(Arrays.asList(arg1, arg2, lit_11));
        
        TreeFormat fm = new TreeFormat();
        
        String actual = fm.describe(substr.getExplainer());
        System.out.println(actual);
        
        /**
         * Expected output:
         * 
            SUBSTRING
            --OPERAND: FROM_UNIXTIME
            ------OPERAND: +
            ----------OPERAND: *
            --------------OPERAND: 123456
            --------------OPERAND: 7
            ----------OPERAND: 8
            ------OPERAND: %Y-%m-%d
            --OPERAND: +
            ------OPERAND: 9
            ------OPERAND: 10
            --OPERAND: 11
                     
         */
        
        String exp = "SUBSTRING\n" +
                     "--OPERAND: FROM_UNIXTIME\n" +
                     "------OPERAND: +\n" +
                     "----------OPERAND: *\n" +
                     "--------------OPERAND: 123456\n" + 
                     "--------------OPERAND: 7\n" + 
                     "----------OPERAND: 8\n" +
                     "------OPERAND: %Y-%m-%d\n" + 
                     "--OPERAND: +\n" +
                     "------OPERAND: 9\n" +
                     "------OPERAND: 10\n" +
                     "--OPERAND: 11";
        
        assertEquals(exp, actual);
               
    }
    
}
