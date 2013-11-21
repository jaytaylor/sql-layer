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
 *  A DirectorySubspace represents the <i>contents</i> of a directory, but it
 *  also remembers the path with which it was opened and offers convenience
 *  methods to operate on the directory at that path.
 *
 * <p>
 *     An instance of DirectorySubspace can be used for all the usual subspace
 *     operations. It can also be used to operate on the directory with which
 *     it was opened.
 * </p>
 */
public class DirectorySubspace extends Subspace {
    private final Tuple path;
    private final byte[] layer;
    private final Directory directory;

    public DirectorySubspace(Tuple path, byte[] prefix, Directory directory) {
        this(path, prefix, directory, null);
    }

    public DirectorySubspace(Tuple path, byte[] prefix, Directory directory, byte[] layer) {
        super(prefix);
        this.path = path;
        this.layer = layer;
        this.directory = directory;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '(' + tupleStr(path) + ", " + printable(getKey()) + ')';
    }

    @Override
    public boolean equals(Object rhs) {
        if(this == rhs) {
            return true;
        }
        if(rhs == null || getClass() != rhs.getClass()) {
            return false;
        }
        DirectorySubspace other = (DirectorySubspace)rhs;
        // TODO: Use path.equals when Tuple.equals() is fixed
        byte[] pathPacked = path != null ? path.pack() : null;
        byte[] otherPacked = other.path != null ? other.path.pack() : null;
        return Arrays.equals(pathPacked, otherPacked) &&
               Arrays.equals(layer, other.layer);
    }

    public Tuple getPath() {
        return path;
    }

    public byte[] getLayer() {
        return layer;
    }

    public boolean exists(Transaction tr) {
        return directory.exists(tr, path);
    }

    public boolean exists(Transaction tr, Tuple subPath) {
        return directory.exists(tr, combine(path, subPath));
    }

    public DirectorySubspace createOrOpen(Transaction tr, Tuple subPath) {
        return createOrOpen(tr, subPath, layer, null);
    }

    public DirectorySubspace createOrOpen(Transaction tr, Tuple subPath, byte[] otherLayer, byte[] prefix) {
        return directory.createOrOpen(tr, combine(path, subPath), otherLayer, prefix);
    }

    public DirectorySubspace open(Transaction tr, Tuple subPath) {
        return open(tr, subPath, layer);
    }

    public DirectorySubspace open(Transaction tr, Tuple subPath, byte[] otherLayer) {
        return directory.open(tr, combine(path, subPath), otherLayer);
    }

    public DirectorySubspace create(Transaction tr, Tuple subPath) {
        return create(tr, subPath, layer);
    }

    public DirectorySubspace create(Transaction tr, Tuple subPath, byte[] otherLayer) {
        return directory.create(tr, combine(path, subPath), otherLayer, null);
    }

    public DirectorySubspace move(Transaction tr, Tuple newPath) {
        return directory.move(tr, path, newPath);
    }

    public DirectorySubspace move(Transaction tr, Tuple oldSubPath, Tuple newSubPath) {
        return directory.move(tr, combine(path, oldSubPath), combine(path, newSubPath));
    }

    public void remove(Transaction tr) {
        directory.remove(tr, path);
    }

    public void remove(Transaction tr, Tuple subPath) {
        directory.remove(tr, combine(path, subPath));
    }

    public void removeIfExists(Transaction tr) {
        directory.removeIfExists(tr, path);
    }

    public void removeIfExists(Transaction tr, Tuple subPath) {
        directory.removeIfExists(tr, combine(path, subPath));
    }

    public List<Object> list(Transaction tr) {
        return directory.list(tr, path);
    }

    public List<Object> list(Transaction tr, Tuple subPath) {
        return directory.list(tr, combine(path, subPath));
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
        if(t == null) {
            return String.valueOf(t);
        }
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
