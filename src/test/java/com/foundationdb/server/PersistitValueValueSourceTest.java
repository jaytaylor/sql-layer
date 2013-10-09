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

package com.foundationdb.server;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSources;
import com.persistit.Persistit;
import com.persistit.Value;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class PersistitValueValueSourceTest {
    @Test
    public void testSkippingNulls() {
        PersistitValueValueSource source = createSource(new ValueInit() {
            @Override
            public void putValues(Value value) {
                value.putNull();
                value.put(1234L);
            }
        });

        readyAndCheck(source, null);

        readyAndCheck(source, MNumeric.BIGINT);
        assertEquals("source value", 1234L, source.getInt64());
    }

    @Test
    public void sameRefTwice() {
        // see https://bugs.launchpad.net/akiban-persistit/+bug/1073357
        // The problem happens when a Value has the same object reference twice -- that is, two objects that are
        // == each other (not just equals). The second object would result in value.getType() == Object, instead of
        // the value's more specific type.
        // A String, a Long and a byte[] walk into a bar. Bartender says to them, "I Value you as customers."
        // The next day they walk back into the same bar, and he says, "now I Object!"


        PersistitValueValueSource source = createSource(new ValueInit() {
            @Override
            public void putValues(Value value) {
                String stringRef = "foo";
                long longVal = 456L;
                byte[] bytesRef = new byte[]{1, 2, 3};

                value.put(stringRef);
                value.put(longVal);
                value.put(bytesRef);

                value.put(stringRef);
                value.put(longVal);
                value.put(bytesRef);
            }
        });

        // first set
        readyAndCheck(source, MString.VARCHAR);
        assertEquals("source value", "foo", source.getString());

        readyAndCheck(source, MNumeric.BIGINT);
        assertEquals("source value", 456L, source.getInt64());

        readyAndCheck(source, MBinary.VARBINARY);
        assertArrayEquals("source value", new byte[] {1, 2, 3}, source.getBytes());

        // second set
        readyAndCheck(source, MString.VARCHAR);
        assertEquals("source value", "foo", source.getString());

        readyAndCheck(source, MNumeric.BIGINT);
        assertEquals("source value", 456L, source.getInt64());

        readyAndCheck(source, MBinary.VARBINARY);
        assertArrayEquals("source value", new byte[] {1, 2, 3}, source.getBytes());
    }

    @Test
    public void getBigDecimalTwice() {
        PersistitValueValueSource source = createSource(new ValueInit() {
            @Override
            public void putValues(Value value) {
                value.putBigDecimal(BigDecimal.ONE);
            }
        });

        source.getReady(MNumeric.DECIMAL.instance(false));
        assertEquals("source value", BigDecimal.ONE, source.getObject());
        assertEquals("source value", BigDecimal.ONE, source.getObject());
    }

    private void readyAndCheck(PersistitValueValueSource source, TClass underlying) {
        source.getReady(underlying == null ? null : underlying.instance(true));
        if (underlying == null) {
            assertTrue("source should be null", source.isNull());
        }
        else {
            assertFalse("source should not be null", source.isNull());
            assertEquals("source UnderlyingType", underlying, ValueSources.tClass(source));
        }
    }

    private PersistitValueValueSource createSource(ValueInit values) {
        Value value = new Value((Persistit)null);
        value.setStreamMode(true);

        values.putValues(value);

        value.setStreamMode(false); // need to reset the Value before reading
        value.setStreamMode(true);

        PersistitValueValueSource source = new PersistitValueValueSource();
        source.attach(value);
        return source;
    }

    private interface ValueInit {
        void putValues(Value value);
    }
}
