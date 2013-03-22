
package com.akiban.util;

import java.util.BitSet;

public final class BitSets {

    public static BitSet of(int... values) {
        BitSet bs = new BitSet(values.length);
        for (int v : values)
            bs.set(v);
        return bs;
    }

    public static BitSet of(boolean[] flags) {
        BitSet bs = new BitSet(flags.length);
        for (int i = 0, max = flags.length; i < max; ++i) {
            if (flags[i])
                bs.set(i);
        }
        return bs;
    }

    private BitSets() {}
}
