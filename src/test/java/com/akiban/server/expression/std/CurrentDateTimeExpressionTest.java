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

import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.AkType;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.expression.Expression;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.ArrayBindings;
import org.junit.Test;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import static org.junit.Assert.assertEquals;

public class CurrentDateTimeExpressionTest
{

    private final DateTime RIGHT_NOW = new DateTime();
    private final DateTime EPOCH = new DateTime(0);
    private Bindings bindings = new ArrayBindings(0);
    private int position = 0;

    @Test
    public void testCurrentDate ()
    {
        test(RIGHT_NOW, AkType.DATE, "yyyy-MM-dd");
        test(EPOCH, AkType.DATE,"yyyy-MM-dd");
    }

    @Test
    public void testCurrentTime ()
    {
        test(RIGHT_NOW, AkType.TIME, "HH:mm:ss");
        test(EPOCH, AkType.TIME, "HH:mm:ss");
    }

    @Test
    public void testCurrentTimestamp ()
    {
        test(RIGHT_NOW, AkType.DATETIME, "yyyy-MM-dd HH:mm:ss");
        test(EPOCH, AkType.DATETIME, "yyyy-MM-dd HH:mm:ss");
    }

    private void test (DateTime when, AkType current_, String strFrm)
    {   
        bindings.set(position, when);
        Expression ex = new CurrentDateTimeExpression(EnvironmentExpressionSetting.CURRENT_DATETIME, position, current_);
        ExpressionEvaluation eval = ex.evaluation();
        eval.of(bindings);

        assertEquals(Extractors.getLongExtractor(current_).getLong(eval.eval()),
                Extractors.getLongExtractor(current_).getLong(when.toString(DateTimeFormat.forPattern(strFrm)))); 
        
    }
}
