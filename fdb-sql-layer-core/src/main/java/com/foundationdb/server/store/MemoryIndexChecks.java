/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.error.ForeignKeyReferencedViolationException;
import com.foundationdb.server.error.ForeignKeyReferencingViolationException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.session.Session;
import com.persistit.Key;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static com.foundationdb.server.store.MemoryStore.BYTES_00;
import static com.foundationdb.server.store.MemoryStore.BYTES_FF;
import static com.foundationdb.server.store.MemoryStore.join;
import static com.foundationdb.server.store.MemoryStore.packKey;
import static com.foundationdb.server.store.MemoryStore.unpackKey;

public class MemoryIndexChecks
{
    public static enum CheckPass
    {
        ROW,
        STATEMENT,
        TRANSACTION,
    }

    public static class PendingChecks
    {
        private final List<IndexCheck> pending;

        public PendingChecks() {
            this.pending = new ArrayList<>();
        }

        public void add(Session session, MemoryTransaction txn, IndexCheck check) {
            pending.add(check);
            performChecks(session, txn, CheckPass.ROW);
        }

        protected void performChecks(Session session, MemoryTransaction txn, CheckPass pass) {
            Iterator<IndexCheck> it = pending.iterator();
            while(it.hasNext()) {
                IndexCheck check = it.next();
                if(check.isReady(session, pass)) {
                    check.query(session, txn);
                    if(!check.check(session, pass)) {
                        throw check.createException(session);
                    }
                    it.remove();
                }
            }
        }

        public void clear() {
            pending.clear();
        }
    }

    public static IndexCheck foreignKeyReferencingCheck(MemoryStoreData storeData,
                                                        Index index,
                                                        ForeignKey foreignKey,
                                                        CheckPass finalPass,
                                                        String operation) {
        byte[][] bounds = makeBounds(storeData, index);
        return new ForeignKeyReferencingCheck(index, bounds[0], bounds[1], foreignKey, finalPass, operation);
    }

    public static IndexCheck foreignKeyNotReferencedCheck(MemoryStoreData storeData,
                                                          Index index,
                                                          boolean isWholeIndex,
                                                          ForeignKey foreignKey,
                                                          boolean isSelfReference,
                                                          CheckPass finalPass,
                                                          String operation) {
        if(isWholeIndex) {
            byte[] begin = packKey(index);
            byte[] end = join(begin, BYTES_FF);
            return new ForeignKeyNotReferencedWholeCheck(index, begin, end, foreignKey, finalPass, operation);
        }
        byte[][] bounds = makeBounds(storeData, index);
        if(isSelfReference) {
            return new ForeignKeyNotReferencedSkipSelfCheck(index, bounds[0], bounds[1], foreignKey, finalPass, operation);
        }
        return new ForeignKeyNotReferencedCheck(index, bounds[0], bounds[1], foreignKey, finalPass, operation);
    }

    //
    // Internal
    //

    private static enum FoundValue
    {
        NONE,
        ONE,
        MULTIPLE,
    }

    public static abstract class IndexCheck {
        protected final Index index;
        protected final byte[] beginKey;
        protected final byte[] endKey;
        protected FoundValue foundValue;

        protected IndexCheck(Index index, byte[] beginKey, byte[] endKey) {
            this.index = index;
            this.beginKey = beginKey;
            this.endKey = endKey;
        }

        /** Perform the index check. */
        public void query(Session session, MemoryTransaction txn) {
            if(endKey == null) {
                byte[] value = txn.get(beginKey);
                foundValue = (value == null) ? FoundValue.NONE : FoundValue.ONE;
            } else {
                Iterator<Entry<byte[], byte[]>> it = txn.getRange(beginKey, endKey);
                if(!it.hasNext()) {
                    foundValue = FoundValue.NONE;
                } else {
                    it.next();
                    foundValue = it.hasNext() ? FoundValue.MULTIPLE : FoundValue.ONE;
                }
            }

        }

        public boolean isReady(Session session, CheckPass pass) {
            return true;
        }

        /** Return <code>true</code> if the check passes. */
        public abstract boolean check(Session session, CheckPass pass);

        /** Create appropriate exception for failed check. */
        public abstract InvalidOperationException createException(Session session);
    }

    private static abstract class ForeignKeyCheck extends IndexCheck
    {
        protected final ForeignKey foreignKey;
        protected final String operation;
        protected final CheckPass finalPass;

        protected ForeignKeyCheck(Index index,
                                  byte[] beginKey,
                                  byte[] endKey,
                                  ForeignKey foreignKey,
                                  CheckPass finalPass,
                                  String operation) {
            super(index, beginKey, endKey);
            this.foreignKey = foreignKey;
            this.finalPass = finalPass;
            this.operation = operation;
        }

        public boolean isReady(Session session, CheckPass pass) {
            return (finalPass.ordinal() <= pass.ordinal());
        }
    }

    private static class ForeignKeyReferencingCheck extends ForeignKeyCheck
    {
        public ForeignKeyReferencingCheck(Index index,
                                          byte[] beginKey,
                                          byte[] endKey,
                                          ForeignKey foreignKey,
                                          CheckPass finalPass,
                                          String operation) {
            super(index, beginKey, endKey, foreignKey, finalPass, operation);
        }

        @Override
        public boolean check(Session session, CheckPass pass) {
            assert foundValue != null;
            return foundValue != FoundValue.NONE;
        }

        @Override
        public InvalidOperationException createException(Session session) {
            Key persistitKey = new Key(null, 2047);
            unpackKey(index, beginKey, persistitKey);
            String key = ConstraintHandler.formatKey(session,
                                                     index,
                                                     persistitKey,
                                                     foreignKey.getReferencingColumns(),
                                                     foreignKey.getReferencedColumns());
            return new ForeignKeyReferencingViolationException(operation,
                                                               foreignKey.getReferencingTable().getName(),
                                                               key,
                                                               foreignKey.getConstraintName().getTableName(),
                                                               foreignKey.getReferencedTable().getName());
        }
    }

    private static class ForeignKeyNotReferencedCheck extends ForeignKeyCheck
    {
        public ForeignKeyNotReferencedCheck(Index index,
                                            byte[] beginKey,
                                            byte[] endKey,
                                            ForeignKey foreignKey,
                                            CheckPass finalPass,
                                            String operation) {
            super(index, beginKey, endKey, foreignKey, finalPass, operation);
        }

        @Override
        public boolean check(Session session, CheckPass pass) {
            assert foundValue != null;
            switch(foundValue) {
                case NONE:
                    return true;
                case ONE:
                    return isOneAllowed(pass);
                case MULTIPLE:
                    return false;
                default:
                    throw new IllegalStateException(foundValue.toString());
            }
        }

        protected boolean isOneAllowed(CheckPass pass) {
            return false;
        }

        @Override
        public InvalidOperationException createException(Session session) {
            Key persistitKey = new Key(null, 2047);
            unpackKey(index, beginKey, persistitKey);
            String key = ConstraintHandler.formatKey(session, index, persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            return new ForeignKeyReferencedViolationException(operation,
                                                              foreignKey.getReferencedTable().getName(),
                                                              key,
                                                              foreignKey.getConstraintName().getTableName(),
                                                              foreignKey.getReferencingTable().getName());
        }
    }

    private static class ForeignKeyNotReferencedSkipSelfCheck extends  ForeignKeyNotReferencedCheck
    {
        public ForeignKeyNotReferencedSkipSelfCheck(Index index,
                                                    byte[] beginKey,
                                                    byte[] endKey,
                                                    ForeignKey foreignKey,
                                                    CheckPass finalPass,
                                                    String operation) {
            super(index, beginKey, endKey, foreignKey, finalPass, operation);
        }

        @Override
        public boolean isOneAllowed(CheckPass pass) {
            return (pass == CheckPass.ROW);
        }
    }

    static class ForeignKeyNotReferencedWholeCheck extends ForeignKeyCheck
    {
        private Key persistitKey;
        private Iterator<Entry<byte[],byte[]>> iterator;

        public ForeignKeyNotReferencedWholeCheck(Index index,
                                                 byte[] beginKey,
                                                 byte[] endKey,
                                                 ForeignKey foreignKey,
                                                 CheckPass finalPass,
                                                 String operation) {
            super(index, beginKey, endKey, foreignKey, finalPass, operation);
        }

        private void ensureKey() {
            if(persistitKey == null) {
                persistitKey = new Key(null, 2047);
            }
        }

        @Override
        public void query(Session session, MemoryTransaction txn) {
            iterator = txn.getRange(beginKey, endKey);
        }

        @Override
        public boolean check(Session session, CheckPass pass) {
            while(iterator.hasNext()) {
                ensureKey();
                byte[] rawKey = iterator.next().getKey();
                unpackKey(index, rawKey, persistitKey);
                if(!ConstraintHandler.keyHasNullSegments(persistitKey, index)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public InvalidOperationException createException(Session session) {
            // Should have been filled in failed check()
            assert persistitKey != null;
            String key = ConstraintHandler.formatKey(session,
                                                     index,
                                                     persistitKey,
                                                     foreignKey.getReferencedColumns(),
                                                     foreignKey.getReferencingColumns());
            return new ForeignKeyReferencedViolationException(operation,
                                                              foreignKey.getReferencedTable().getName(),
                                                              key,
                                                              foreignKey.getConstraintName().getTableName(),
                                                              foreignKey.getReferencingTable().getName());
        }
    }

    private static byte[][] makeBounds(MemoryStoreData storeData, Index index) {
        packKey(storeData);
        byte[] begin;
        byte[] end = null;
        // Normal case, reference does not contain all columns
        if(storeData.persistitKey.getDepth() < index.getAllColumns().size()) {
            begin = join(storeData.rawKey, BYTES_00);
            end = join(storeData.rawKey, BYTES_FF);
        } else {
            // Exactly matches index, including HKey columns
            begin = join(storeData.rawKey);
        }
        return new byte[][]{ begin, end };
    }
}
