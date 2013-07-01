/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.persistitadapter.indexcursor.IterationHelper;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ShareHolder;
import com.foundationdb.KeyValue;
import com.foundationdb.tuple.Tuple;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.KeyShim;
import com.persistit.Persistit;
import com.persistit.Value;

import java.util.Iterator;

import static com.persistit.Key.Direction.EQ;
import static com.persistit.Key.Direction.GT;
import static com.persistit.Key.Direction.GTEQ;
import static com.persistit.Key.Direction.LT;
import static com.persistit.Key.Direction.LTEQ;

public class FDBIterationHelper implements IterationHelper
{
    private final FDBAdapter adapter;
    private final IndexRowType rowType;
    private final ShareHolder<PersistitIndexRow> row;
    private final Key key;
    private final Value value;
    // Initialized upon traversal
    private KeyValue lastKV;
    private long lastKeyGen;
    private Direction itDir;
    private Iterator<KeyValue> it;
    // Only instantiated for logical traversal
    private Key spareKey;


    public FDBIterationHelper(FDBAdapter adapter, IndexRowType rowType) {
        this.adapter = adapter;
        this.rowType = rowType.physicalRowType();
        this.row = new ShareHolder<>(adapter.takeIndexRow(this.rowType));
        this.key = adapter.createKey();
        this.value = new Value((Persistit)null);
    }


    //
    // Iteration helper
    //

    @Override
    public Row row() {
        assert (lastKV != null) : "Called for chopped key (or before iterating)"; // See advanceLogical() for former
        PersistitIndexRow row = unsharedRow().get();
        // updateKey() called from advance
        updateValue();
        row.copyFromKeyValue(key, value);
        return row;
    }

    @Override
    public void openIteration() {
        // None, iterator created on demand
    }

    @Override
    public void closeIteration() {
        if(row.isHolding()) {
            if(!row.isShared()) {
                adapter.returnIndexRow(row.get());
            }
            row.release();
        }
    }

    @Override
    public Key key() {
        return key;
    }

    @Override
    public void clear() {
        key.clear();
        value.clear();
        lastKV = null;
        lastKeyGen = -1;
        itDir = null;
        it = null;
    }

    @Override
    public boolean next(boolean deep) {
        checkIterator(Direction.GT, deep);
        return advance(deep);
    }

    @Override
    public boolean prev(boolean deep) {
        checkIterator(Direction.LT, deep);
        return advance(deep);
    }

    @Override
    public boolean traverse(Direction dir, boolean deep) {
        checkIterator(dir, deep);
        return advance(deep);
    }


    //
    // Internal
    //

    private ShareHolder<PersistitIndexRow> unsharedRow() {
        if(row.isEmpty() || row.isShared()) {
            row.hold(adapter.takeIndexRow(rowType));
        }
        return row;
    }

    private boolean advance(boolean deep) {
        return deep ? advanceDeep() : advanceLogical();
    }

    /** Advance iterator with pure physical (i.e. key order) traversal. */
    private boolean advanceDeep() {
        if(it.hasNext()) {
            lastKV = it.next();
            updateKey();
            return true;
        }
        return false;
    }

    /** Advance iterator with logical style (i.e. non-deep) traversal. Emulates Exchange shallow behavior. */
    private boolean advanceLogical() {
        if(spareKey == null) {
            spareKey = adapter.createKey();
        }
        key.copyTo(spareKey);
        int parentIndex = KeyShim.previousElementIndex(key, spareKey.getEncodedSize());
        if(parentIndex < 0) {
            parentIndex = 0;
        }
        while(it.hasNext()) {
            lastKV = it.next();
            updateKey();
            boolean matches = (spareKey.compareKeyFragment(key, 0, parentIndex) == 0);
            if(matches) {
                int originalSize = key.getEncodedSize();
                int nextIndex = KeyShim.nextElementIndex(key, parentIndex);
                // Note: Proper emulation would require looking up this (possibly fake) key. Server doesn't use that.
                if(nextIndex != originalSize) {
                    key.setEncodedSize(nextIndex);
                    lastKV = null;
                    value.clear();
                }
                return true;
            }
        }
        // No match, restore original key
        spareKey.copyTo(key);
        return false;
    }

    /** Check current iterator matches direction and recreate if not. */
    private void checkIterator(Direction dir, boolean deep) {
        final boolean keyGenMatches = (lastKeyGen == key.getGeneration());
        if((itDir != dir) || !keyGenMatches) {
            // If the last key we returned hasn't changed and moving in the same direction, new iterator isn't needed.
            if(keyGenMatches) {
                if((itDir == GTEQ && dir == GT) || (itDir == LTEQ && dir == LT)) {
                    itDir = dir;
                    return;
                }
            }
            final int saveSize = key.getEncodedSize();
            final boolean exact = dir == EQ || dir == GTEQ || dir == LTEQ;
            final boolean reverse = (dir == LT) || (dir == LTEQ);
            if(!KeyShim.isSpecial(key)) {
                if(exact) {
                    if(reverse && !deep) {
                        // exact, reverse, logical: want to see current key or a child
                        // Note: child won't be returned, but current key will be synthesized by advanceLogical()
                        KeyShim.nudgeRight(key);
                    }
                } else {
                    if(reverse) {
                        // Non-exact, reverse: do not want to see current key
                        KeyShim.nudgeLeft(key);
                    } else {
                        if(deep) {
                            // Non-exact, forward, deep: do not want to see current key
                            KeyShim.nudgeDeeper(key);
                        } else {
                            // Non-exact, forward, logical: do not want to see current key or any children
                            KeyShim.nudgeRight(key);
                        }
                    }
                }
            }

            it = adapter.getUnderlyingStore().indexIterator(adapter.getSession(), rowType.index(), key, exact, reverse);
            key.setEncodedSize(saveSize);
            lastKeyGen = key.getGeneration();
            itDir = dir;
        }
    }

    private void updateKey() {
        byte[] keyBytes = Tuple.fromBytes(lastKV.getKey()).getBytes(2);
        System.arraycopy(keyBytes, 0, key.getEncodedBytes(), 0, keyBytes.length);
        key.setEncodedSize(keyBytes.length);
    }

    private void updateValue() {
        byte[] valueBytes = lastKV.getValue();
        value.clear();
        value.putByteArray(valueBytes);
    }
}
