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

package com.akiban.server.types3;

import com.akiban.util.BitSets;

import java.util.BitSet;

public final class InputSetFlags {

    public static final InputSetFlags ALL_OFF = new InputSetFlags(new boolean[0], false);

    public boolean get(int i) {
        if (i < 0)
            throw new IllegalArgumentException("out of range: " + i);
        return (i < nPositions) ? positionals.get(i) : varargs;
    }

    public InputSetFlags(BitSet positionals, int nPositions, boolean varargs) {
        this.positionals = new BitSet(nPositions);
        this.positionals.or(positionals);
        this.nPositions = nPositions;
        this.varargs = varargs;
    }

    @Override
    public String toString() {
        // for length, assume each is "false", and each also has a ", " afterwards. That's seven chars times
        // positionals.length(), plus another 5 for the vararg, plus "..." after the vararg.
        StringBuilder sb = new StringBuilder( (7 * positionals.length()) + 5 + 2);
        for (int i = 0; i < nPositions; ++i)
            sb.append(positionals.get(i)).append(", ");
        sb.append(varargs).append("...");
        return sb.toString();
    }

    public InputSetFlags(boolean[] positionals, boolean varargs) {
        this(BitSets.of(positionals), positionals.length, varargs);
    }

    private final BitSet positionals;
    private final int nPositions;
    private final boolean varargs;

    public static class Builder {

        public void set(int pos, boolean value) {
            if(pos < 0)
                throw new IllegalArgumentException("out of range: " + pos);
            nPositions = Math.max(nPositions, pos+1);
            bitSet.set(pos, value);
        }

        public void setVararg(boolean value) {
            this.varargValue = value;
        }

        public InputSetFlags get() {
            if ( (!varargValue) && (bitSet.length() == 0))
                return ALL_OFF;
            return new InputSetFlags(bitSet, nPositions, varargValue);
        }

        private boolean varargValue;
        private int nPositions = 0;
        private BitSet bitSet = new BitSet();
    }
}
