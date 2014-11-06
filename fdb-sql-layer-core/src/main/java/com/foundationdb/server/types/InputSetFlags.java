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

package com.foundationdb.server.types;

import com.foundationdb.util.BitSets;

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
