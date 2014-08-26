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
package com.foundationdb.qp.storeadapter;

import com.foundationdb.qp.storeadapter.indexcursor.IterationHelper;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.KeyShim;
import com.persistit.Persistit;
import com.persistit.Value;
import com.persistit.KeyState;

import static com.persistit.Key.Direction.EQ;
import static com.persistit.Key.Direction.GT;
import static com.persistit.Key.Direction.GTEQ;
import static com.persistit.Key.Direction.LT;
import static com.persistit.Key.Direction.LTEQ;

public class FDBIterationHelper implements IterationHelper
{
    private final FDBAdapter adapter;
    private final IndexRowType rowType;
    private final FDBStoreData storeData;
    // Initialized upon traversal
    private long lastKeyGen;
    private Direction itDir;
    // Only instantiated for logical traversal
    private Key spareKey;


    public FDBIterationHelper(FDBAdapter adapter, IndexRowType rowType) {
        this.adapter = adapter;
        this.rowType = rowType.physicalRowType();
        this.storeData = adapter.getUnderlyingStore().createStoreData(adapter.getSession(), rowType.index());
        this.storeData.persistitValue = new Value((Persistit)null);
    }


    //
    // Iteration helper
    //

    @Override
    public Row row() {
        assert (storeData.rawKey != null) : "Called for chopped key (or before iterating)"; // See advanceLogical() for former
        PersistitIndexRow row = adapter.takeIndexRow(rowType);
        // updateKey() called from advance
        updateValue();
        row.copyFrom(storeData.persistitKey, storeData.persistitValue);
        return row;
    }

    @Override
    public void openIteration() {
        // None, iterator created on demand
    }

    @Override
    public void closeIteration() {
        //adapter.returnIndexRow(row);
    }

    @Override
    public Key key() {
        return storeData.persistitKey;
    }

    @Override
    public void clear() {
        storeData.persistitKey.clear();
        storeData.persistitValue.clear();
        lastKeyGen = -1;
        itDir = null;
    }

    @Override
    public boolean next(boolean deep) {
        try {
            checkIterator(Direction.GT, deep);
            return advance(deep);
        } catch (Exception e) {
            throw FDBAdapter.wrapFDBException(adapter.getSession(), e);
        }
    }

    @Override
    public boolean prev(boolean deep) {
        try {
            checkIterator(Direction.LT, deep);
            return advance(deep);
        } catch (Exception e) {
            throw FDBAdapter.wrapFDBException(adapter.getSession(), e);
        }
    }

    @Override
    public boolean traverse(Direction dir, boolean deep) {
        try {
            checkIterator(dir, deep);
            return advance(deep);
        } catch (Exception e) {
            throw FDBAdapter.wrapFDBException(adapter.getSession(), e);
        }
    }

    @Override
    public void preload(Direction dir, boolean deep) {
        checkIterator(dir, deep);
    }

    //
    // Internal
    //

    private boolean advance(boolean deep) {
        return deep ? advanceDeep() : advanceLogical();
    }

    /** Advance iterator with pure physical (i.e. key order) traversal. */
    private boolean advanceDeep() {
        if(storeData.next()) {
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
        storeData.persistitKey.copyTo(spareKey);
        int parentIndex = KeyShim.previousElementIndex(storeData.persistitKey, spareKey.getEncodedSize());
        if(parentIndex < 0) {
            parentIndex = 0;
        }
        while(storeData.next()) {
            updateKey();
            boolean matches = (spareKey.compareKeyFragment(storeData.persistitKey, 0, parentIndex) == 0);
            if(matches) {
                int originalSize = storeData.persistitKey.getEncodedSize();
                int nextIndex = KeyShim.nextElementIndex(storeData.persistitKey, parentIndex);
                if(nextIndex > 0) {
                    // Note: Proper emulation would require looking up this (possibly fake) key. Server doesn't need it.
                    if(nextIndex != originalSize) {
                        storeData.persistitKey.setEncodedSize(nextIndex);
                        storeData.persistitValue.clear();
                    }
                    return true;
                }
                // else we found a non-matching prefix (e.g. iterated from child to orphan cousin
            }
        }
        // No match, restore original key
        spareKey.copyTo(storeData.persistitKey);
        return false;
    }

    /** Check current iterator matches direction and recreate if not. */
    private void checkIterator(Direction dir, boolean deep) {
        final boolean keyGenMatches = (lastKeyGen == storeData.persistitKey.getGeneration());
        if((itDir != dir) || !keyGenMatches) {
            // If the last key we returned hasn't changed and moving in the same direction, new iterator isn't needed.
            if(keyGenMatches) {
                if((itDir == GTEQ && dir == GT) || (itDir == LTEQ && dir == LT)) {
                    itDir = dir;
                    return;
                }
            }
            final int saveSize = storeData.persistitKey.getEncodedSize();
            final boolean exact = dir == EQ || dir == GTEQ || dir == LTEQ;
            final boolean reverse = (dir == LT) || (dir == LTEQ);
            KeyState saveState = null;
            
            assert storeData.nudged == null;
            if(!KeyShim.isSpecial(storeData.persistitKey)) {
                if(exact) {
                    if(reverse && !deep) {
                        // exact, reverse, logical: want to see current key or a child
                        // Note: child won't be returned, but current key will be synthesized by advanceLogical()
                        saveState = new KeyState(storeData.persistitKey);
                        KeyShim.nudgeRight(storeData.persistitKey);
                        storeData.nudged = FDBStoreData.NudgeDir.RIGHT_NO_STRINC;
                    }
                } else {
                    if(reverse) {
                        // Non-exact, reverse: do not want to see current key
                        KeyShim.nudgeLeft(storeData.persistitKey);
                        storeData.nudged = FDBStoreData.NudgeDir.LEFT;
                    } else {
                        if(deep) {
                            // Non-exact, forward, deep: do not want to see current key
                            KeyShim.nudgeDeeper(storeData.persistitKey);
                            storeData.nudged = FDBStoreData.NudgeDir.DEEPER;
                        } else {
                            // Non-exact, forward, logical: do not want to see current key or any children
                            saveState = new KeyState(storeData.persistitKey);
                            KeyShim.nudgeRight(storeData.persistitKey);
                            storeData.nudged = FDBStoreData.NudgeDir.RIGHT_STRINC;
                        }
                    }
                }
            }
            
            adapter.getUnderlyingStore().indexIterator(adapter.getSession(), storeData,
                                                       exact, reverse);
            storeData.nudged = null;
            if (saveState != null) {
                saveState.copyTo(storeData.persistitKey);
            }
            storeData.persistitKey.setEncodedSize(saveSize);
            lastKeyGen = storeData.persistitKey.getGeneration();
            itDir = dir;
        }
    }

    private void updateKey() {
        FDBStoreDataHelper.unpackKey(storeData);
        lastKeyGen = storeData.persistitKey.getGeneration();
    }

    private void updateValue() {
        FDBStoreDataHelper.unpackValue(storeData);
    }
}
