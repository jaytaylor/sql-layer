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

package com.akiban.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ShareHolderTest {

    @Test
    public void totallyEmpty() {
        assertEquals("shared state", false, new ShareHolder<Shareable>().isHolding());
    }

    @Test
    public void testSharing() {
        Shareable shareable = new DummyShareable();
        shareable.acquire();

        ShareHolder<Shareable> holder = new ShareHolder<Shareable>();
        assertEquals("shared state A", false, holder.isHolding());
        assertEquals("shareable.isShared", false, shareable.isShared());

        holder.hold(shareable);
        assertEquals("shared state B", true, holder.isHolding());
        assertEquals("shareable.isShared", true, shareable.isShared());

        holder.release();
        assertEquals("shared state A", false, holder.isHolding());
        assertEquals("shareable.isShared", false, shareable.isShared());
    }

    @Test
    public void releaseWhenNotHeld() {
        new ShareHolder<Shareable>().release();
    }

    @Test
    public void holdNull() {
        ShareHolder<Shareable> holder = new ShareHolder<Shareable>();
        holder.hold(null);
    }

    /**
     * A dummy class which can be shared or released, but which only counts as being shared as long as
     * the number of shares is >= the number given in its constructor. For instance, if that number is 0,
     * this object is shared by default. If it's 2, you'll have to call {@linkplain #acquire} twice for it to
     * be shared.
     */
    private static class DummyShareable implements Shareable {

        @Override
        public void acquire() {
            ++ownedBy;
        }

        @Override
        public boolean isShared() {
            return ownedBy > 1;
        }

        @Override
        public void release() {
            assert ownedBy >= 0 : ownedBy;
            if (ownedBy > 0)
                --ownedBy;
        }

        private int ownedBy = 0;
    }
}
