/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server;

import com.akiban.server.types3.pvalue.PUnderlying;
import com.persistit.Persistit;
import com.persistit.Value;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class PersistitValuePValueSourceTest {
    @Test
    public void testSkippingNulls() {
        PersistitValuePValueSource source = createSource(new ValueInit() {
            @Override
            public void putValues(Value value) {
                value.putNull();
                value.put(1234L);
            }
        });

        readyAndCheck(source, null);

        readyAndCheck(source, PUnderlying.INT_64);
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


        PersistitValuePValueSource source = createSource(new ValueInit() {
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
        readyAndCheck(source, PUnderlying.STRING);
        assertEquals("source value", "foo", source.getString());

        readyAndCheck(source, PUnderlying.INT_64);
        assertEquals("source value", 456L, source.getInt64());

        readyAndCheck(source, PUnderlying.BYTES);
        assertArrayEquals("source value", new byte[] {1, 2, 3}, source.getBytes());

        // second set
        readyAndCheck(source, PUnderlying.STRING);
        assertEquals("source value", "foo", source.getString());

        readyAndCheck(source, PUnderlying.INT_64);
        assertEquals("source value", 456L, source.getInt64());

        readyAndCheck(source, PUnderlying.BYTES);
        assertArrayEquals("source value", new byte[] {1, 2, 3}, source.getBytes());
    }

    private void readyAndCheck(PersistitValuePValueSource source, PUnderlying pUnderlying) {
        source.getReady();
        if (pUnderlying == null) {
            assertTrue("source should be null", source.isNull());
        }
        else {
            assertFalse("source should not be null", source.isNull());
            assertEquals("source PUnderlying", pUnderlying, source.getUnderlyingType());
        }
    }

    private PersistitValuePValueSource createSource(ValueInit values) {
        Value value = new Value((Persistit)null);
        value.setStreamMode(true);
        values.putValues(value);
        PersistitValuePValueSource source = new PersistitValuePValueSource();
        source.attach(value);
        return source;
    }

    private interface ValueInit {
        void putValues(Value value);
    }
}
