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

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

import java.util.Arrays;
import java.util.List;

import static com.foundationdb.tuple.ByteArrayUtil.printable;

/**
 *  A DirectorySubspace represents the *contents* of a directory, but it also
 * remembers the path with which it was opened and offers convenience methods
 * to operate on the directory at that path.
 */
public class DirectorySubspace extends Subspace {
    private final Tuple path;
    private final Directory directoryLayer;
    private final byte[] layer;

    public DirectorySubspace(Tuple path, byte[] prefix, Directory directoryLayer/*=directory*/, byte[] layer/*=None*/) {
        super(prefix);
        this.path = path;
        this.directoryLayer = directoryLayer;
        this.layer = layer;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '(' + tupleStr(path) + ", " + printable(getKey()) + ')';
    }

    public void check_layer(byte[] otherLayer) {
        if(layer != null && otherLayer != null && !Arrays.equals(layer, otherLayer)) {
            throw new IllegalArgumentException("The directory was created with an incompatible layer.");
        }
    }

    public DirectorySubspace create_or_open(Transaction tr,
                                            Tuple subPath,
                                            byte[] layer/*=None*/,
                                            byte[] prefix/*=None*/) {
        return directoryLayer.create_or_open(tr, combine(this.path, subPath), layer, prefix, true, true);
    }

    public DirectorySubspace open(Transaction tr, Tuple subPath, byte[] layer/*=None*/) {
        return directoryLayer.open(tr, combine(path, subPath), layer);
    }

    public DirectorySubspace create(Transaction tr, Tuple subPath, byte[] layer/*=None*/) {
        return directoryLayer.create(tr, combine(path, subPath), layer, null);
    }

    public DirectorySubspace move(Transaction tr, Tuple new_path) {
        return directoryLayer.move(tr, path, new_path);
    }

    public void remove(Transaction tr) {
        directoryLayer.remove(tr, path);
    }

    public List<byte[]> list(Transaction tr) {
        return directoryLayer.list(tr, path);
    }


    //
    // Helpers
    //

    public static Tuple combine(Tuple a, Tuple b) {
        Tuple out = Tuple.fromItems(a);
        for(Object o : b) {
            out = out.addObject(o);
        }
        return out;
    }

    public static String tupleStr(Tuple t) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for(Object o : t) {
            if(!first) {
                sb.append(',');
            } else {
                first = false;
            }
            if(o instanceof byte[]) {
                sb.append(printable((byte[])o));
            } else {
                sb.append(o);
            }
        }
        sb.append(')');
        return sb.toString();
    }
}
