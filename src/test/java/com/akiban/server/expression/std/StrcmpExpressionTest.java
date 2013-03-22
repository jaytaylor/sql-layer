/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import org.junit.*;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
        
/**
 *
 * @author louisli
 */
public class StrcmpExpressionTest extends ComposedExpressionTestBase
{
    private String strShort = "stringshort";
    private String strLong = "stringlonglonglonglong";
    private String strFirst = "A starts with";
    private String strSecond = "B starts with";
    private String sentenceCapitalized = "The brOwn Fox jumped over the lazy dog.";
    private String sentenceLowercase = "the brown fox jumped over the lazy dog.";
    
    /* Note that comparison is based on Unicode ordering */
    private String strParentheses = "((";

    @Test
    public void testEqual()
    {
        Expression compareEqualsOne = new StrcmpExpression(ExprUtil.lit(strShort), ExprUtil.lit(strShort));
        assertEquals(0, compareEqualsOne.evaluation().eval().getInt());
        
        Expression compareEqualsTwo = new StrcmpExpression(ExprUtil.lit(sentenceCapitalized), ExprUtil.lit(sentenceCapitalized));
        assertEquals(0, compareEqualsTwo.evaluation().eval().getInt());

        Expression compareNotEqual = new StrcmpExpression(ExprUtil.lit(sentenceCapitalized), ExprUtil.lit(sentenceLowercase));
        assertThat(compareNotEqual.evaluation().eval().getInt(), not(0L));
    }
    
    @Test
    public void testAscendingOrder()
    {
        Expression compareLessOne = new StrcmpExpression(ExprUtil.lit(strParentheses), ExprUtil.lit(strShort));
        assertEquals(-1, compareLessOne.evaluation().eval().getInt());
        
        Expression compareLessTwo = new StrcmpExpression(ExprUtil.lit(strFirst), ExprUtil.lit(strSecond));
        assertEquals(-1, compareLessTwo.evaluation().eval().getInt());
        
        // Capital letters come before lowercase letters
        Expression compareLessCapitalization = new StrcmpExpression(ExprUtil.lit(sentenceCapitalized), ExprUtil.lit(sentenceLowercase));
        assertEquals(-1, compareLessCapitalization.evaluation().eval().getInt());
    }
    
    @Test
    public void testDescendingOrder()
    {
        Expression compareGreaterOne = new StrcmpExpression(ExprUtil.lit(strShort), ExprUtil.lit(strParentheses));
        assertEquals(1, compareGreaterOne.evaluation().eval().getInt());
        
        Expression compareGreaterTwo = new StrcmpExpression(ExprUtil.lit(strSecond), ExprUtil.lit(strFirst));
        assertEquals(1, compareGreaterTwo.evaluation().eval().getInt());
        
        Expression compareGreaterCapitalization = new StrcmpExpression(ExprUtil.lit(sentenceLowercase), ExprUtil.lit(sentenceCapitalized));
        assertEquals(1, compareGreaterCapitalization.evaluation().eval().getInt());
    }
    
    @Test
    public void testCaseSensitivity()
    {
        // Expect that lowercase words come first lexicographically
        Expression compareSensitivity = new StrcmpExpression((ExprUtil.lit("foo")), ExprUtil.lit("FOO"));
        assertEquals(1, compareSensitivity.evaluation().eval().getInt());
    }
    
    @Test
    public void testNull()
    {
        Expression leftNull = new StrcmpExpression(ExprUtil.constNull(), ExprUtil.lit("foo"));
        Expression rightNull = new StrcmpExpression(ExprUtil.lit("foo"), ExprUtil.constNull());
        Expression allNull = new StrcmpExpression(ExprUtil.constNull(), ExprUtil.constNull());
        assertTrue(leftNull.evaluation().eval().isNull());
        assertTrue(rightNull.evaluation().eval().isNull());
        assertTrue(allNull.evaluation().eval().isNull());
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return StrcmpExpression.COMPOSER;
    }

    @Override
    protected boolean alreadyExc()
    {
        return false;
    }

}
