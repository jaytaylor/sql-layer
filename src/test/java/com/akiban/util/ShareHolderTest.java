
package com.akiban.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ShareHolderTest {

    @Test
    public void totallyEmpty() {
        assertEquals("shared state", false, new ShareHolder<>().isHolding());
    }

    @Test
    public void testSharing() {
        Shareable shareable = new DummyShareable();
        shareable.acquire();

        ShareHolder<Shareable> holder = new ShareHolder<>();
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
        new ShareHolder<>().release();
    }

    @Test
    public void holdNull() {
        ShareHolder<Shareable> holder = new ShareHolder<>();
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
