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

package com.akiban.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class ShareHolderTest {

    @Test
    public void totallyEmpty() {
        assertEquals("shared state", false, new ShareHolder<Shareable>().isShared());
    }

    @Test
    public void testSharing() {
        Shareable shareable = new DummyShareable(1);
        ShareHolder<Shareable> holder = new ShareHolder<Shareable>();
        holder.reserve(shareable);
        assertEquals("shared state A", false, holder.isShared());
        holder.share();
        assertEquals("shared state B", true, holder.isShared());
        holder.release();
        assertEquals("shared state A", false, holder.isShared());
    }

    @Test
    public void autoShared() {
        ShareHolder<Shareable> holder = new ShareHolder<Shareable>();
        holder.reserve(new DummyShareable(0));
        assertEquals("shared state", true, holder.isShared());
    }

    @Test(expected = IllegalStateException.class)
    public void shareIllegally() {
        new ShareHolder<Shareable>().share();
    }

    @Test(expected = IllegalStateException.class)
    public void releaseIllegally() {
        new ShareHolder<Shareable>().release();
    }

    @Test(expected = IllegalArgumentException.class)
    public void reserveNull() {
        ShareHolder<Shareable> holder = new ShareHolder<Shareable>();
        holder.reserve(null);
    }

    /**
     * A dummy class which can be shared or released, but which only counts as being shared as long as
     * the number of shares is >= the number given in its constructor. For instance, if that number is 0,
     * this object is shared by default. If it's 2, you'll have to call {@linkplain #share} twice for it to
     * be shared.
     */
    private static class DummyShareable implements Shareable {

        @Override
        public void share() {
            ++sharedBy;
        }

        @Override
        public boolean isShared() {
            return sharedBy >= sharedWhen;
        }

        @Override
        public void release() {
            --sharedBy;
        }

        private DummyShareable(int sharedWhen) {
            this.sharedWhen = sharedWhen;
        }

        private final int sharedWhen;
        private int sharedBy;
    }
}
