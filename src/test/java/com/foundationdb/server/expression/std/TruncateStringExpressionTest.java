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

import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.NullValueSource;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TruncateStringExpressionTest
{
    protected ValueSource truncate(ValueSource source, int length) {
        Expression expression = new TruncateStringExpression(length,
                                                             new LiteralExpression(source));
        return expression.evaluation().eval();
    }

    protected String truncate(String string, int length) {
        ValueSource source = new ValueHolder(AkType.VARCHAR, string);
        ValueSource result = truncate(source, length);
        return result.getString();
    }

    @Test
    public void testNull() {
        assertTrue("result is null", 
                   truncate(NullValueSource.only(), 128).isNull());
    }
    
    @Test
    public void testSame() {
        String value;

        value = "";
        assertEquals(value, truncate(value, 100));

        value = "some string";
        assertEquals(value, truncate(value, 100));

        value = "123";
        assertEquals(value, truncate(value, 3));
    }

    @Test
    public void testTruncate() {
        assertEquals("123",
                     truncate("123456", 3));
    }
}
