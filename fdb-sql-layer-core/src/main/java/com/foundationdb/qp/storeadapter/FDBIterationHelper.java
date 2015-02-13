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
import com.foundationdb.qp.row.IndexRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.KeyShim;
import com.persistit.Persistit;
import com.persistit.Value;

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
        IndexRow row = adapter.takeIndexRow(rowType);
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
    public Key endKey() {
        return storeData.endKey;
    }
    
    @Override
    public void clear() {
        storeData.persistitKey.clear();
        storeData.persistitValue.clear();
        lastKeyGen = -1;
        itDir = null;
    }

    @Override
    public boolean traverse(Direction dir) {
        try {
            checkIterator(dir, true);
            return advance();
        } catch (Exception e) {
            throw FDBAdapter.wrapFDBException(adapter.getSession(), e);
        }
    }

    @Override
    public void preload(Direction dir, boolean endInclusive) {
        checkIterator(dir, endInclusive);
    }

    //
    // Internal
    //

    /** Advance iterator with pure physical (i.e. key order) traversal. */
    private boolean advance() {
        if(storeData.next()) {
            updateKey();
            return true;
        }
        return false;
    }

    /** Check current iterator matches direction and recreate if not. */
    private void checkIterator(Direction dir, boolean endInclusive) {
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
            final boolean exactEnd = endInclusive;

            assert storeData.nudgeDir == null;
            if(!KeyShim.isSpecial(storeData.persistitKey) && !exact) {
                if(reverse) {
                    // Non-exact, reverse: do not want to see current key
                    KeyShim.nudgeLeft(storeData.persistitKey);
                    storeData.nudgeDir = FDBStoreData.NudgeDir.LEFT;
                } else {
                    // Non-exact, forward, deep: do not want to see current key
                    KeyShim.nudgeDeeper(storeData.persistitKey);
                    storeData.nudgeDir = FDBStoreData.NudgeDir.DEEPER;
                }
            }

            adapter.getUnderlyingStore().indexIterator(adapter.getSession(), storeData,
                                                       true, exact, exactEnd, reverse,
                                                       adapter.scanOptions());
            storeData.nudgeDir = null;
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
