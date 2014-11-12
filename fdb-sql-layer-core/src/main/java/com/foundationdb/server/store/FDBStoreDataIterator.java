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

package com.foundationdb.server.store;

import java.util.Iterator;

/**
 * A storage iterator is an <code>Iterator<Void></code> is stored in the
 * <code>iterator</code> field of a <code>FDBStoreData</code>. When advanced, it modifies
 * the state of the <code>FDBStoreData</code> so that {@link FDBStore#unpackTuple}
 * can be called to fill the <code>persistitKey</code> field and, in the case of
 * Group iterators, {@link FDBStore#expandRowData} can be called to fill a
 * <code>RowData</code>.<p>
 * Normally, this is just a matter of copying the key/value pair of byte arrays into
 * the <code>rawKey</code> and <code>rawValue</code> fields.
 */
public abstract class FDBStoreDataIterator implements Iterator<Void> {
    protected final FDBStoreData storeData;

    public FDBStoreDataIterator(FDBStoreData storeData) {
        this.storeData = storeData;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public abstract void close();
}
