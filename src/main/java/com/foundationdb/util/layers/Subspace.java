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

package com.foundationdb.util.layers;

import com.foundationdb.Range;
import com.foundationdb.tuple.Tuple;

import java.util.Arrays;

import static com.foundationdb.tuple.ByteArrayUtil.join;
import static com.foundationdb.tuple.ByteArrayUtil.printable;

/**
 * Provides a Subspace class to defines subspaces of keys.
 *
 * <p>
 *     Subspaces should be used to manage namespaces for application data.
 *     The use of distinct subspaces helps to avoid conflicts among keys.
 * </p>
 *
 * <p>
 *     Subspaces employ the tuple layer. A Subspace is initialized with a identifier in
 *     the form of a tuple (and, optionally, a raw prefix). An instance of Subspace
 *     stores the identifier and automatically adds it as a prefix when encoding tuples
 *     into keys. Likewise, it removes the prefix when decoding keys. The class methods
 *     are similar to those of the tuple layer, augmented to manage the prefix.
 * </p>
*/
public class Subspace
{
    private static final Tuple EMPTY_TUPLE = Tuple.from();
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final byte[] rawPrefix;

    public Subspace() {
        this(EMPTY_TUPLE, EMPTY_BYTES);
    }

    public Subspace(Tuple prefixTuple) {
        this(prefixTuple, EMPTY_BYTES);
    }

    public Subspace(Tuple prefixTuple, byte[] rawPrefix) {
        this.rawPrefix = join(rawPrefix, prefixTuple.pack());
    }

    @Override
    public String toString() {
        return "Subspace(rawPrefix=" + printable(rawPrefix) + ")";
    }

    public Subspace get(Tuple name) {
        return new Subspace(name, rawPrefix);
    }

    public byte[] getKey() {
        return rawPrefix;
    }

    public byte[] pack() {
        return rawPrefix;
    }

    public byte[] pack(Tuple tuple) {
        return join(rawPrefix, tuple.pack());
    }

    public Tuple unpack(byte[] key) {
        assert startsWith(rawPrefix, key);
        return Tuple.fromBytes(Arrays.copyOfRange(key, rawPrefix.length, key.length));
    }

    public Range range() {
        return range(EMPTY_TUPLE);
    }

    public Range range(Tuple tuple) {
        Range p = tuple.range();
        return new Range(join(rawPrefix, p.begin), join(rawPrefix, p.end));
    }

    public boolean contains(byte[] key) {
        return startsWith(rawPrefix, key);
    }

    public Subspace subspace(Tuple tuple) {
        return new Subspace(tuple, rawPrefix);
    }


    private static boolean startsWith(byte[] prefix, byte[] other) {
        if(other.length < prefix.length) {
            return false;
        }
        for(int i = 0; i < prefix.length; ++i) {
            if(prefix[i] != other[i]) {
                return false;
            }
        }
        return true;
    }
}
