/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server;

import static com.akiban.server.service.tree.TreeService.STATUS_TREE_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.TransactionalCache;
import com.persistit.Value;
import com.persistit.exception.ConversionException;
import com.persistit.exception.PersistitException;

public class PersistitTransactionalCacheTableStatusCache extends TransactionalCache implements TableStatusCache {

    private final static int INITIAL_MAP_SIZE = 1000;

    private final TreeService treeService;

    public PersistitTransactionalCacheTableStatusCache(SessionService sessionService, final Persistit db, final TreeService treeService) {
        super(db);
        this.treeService = treeService;
        this.sessionService = sessionService;
    }
    
    private PersistitTransactionalCacheTableStatusCache(final PersistitTransactionalCacheTableStatusCache tsc) {
        super(tsc);
        treeService = tsc.treeService;
        for (final Entry<Integer, InnerTableStatus> entry : tsc.tableStatusMap.entrySet()) {
            tableStatusMap.put(entry.getKey(), new InnerTableStatus(entry.getValue()));
        }
        this.sessionService = tsc.sessionService;
    }

    private static final long serialVersionUID = 2823468378367226075L;

    private final static byte INCREMENT = 1;
    private final static byte DECREMENT = 2;
    private final static byte TRUNCATE = 3;
    private final static byte AUTO_INCREMENT = 4;
    private final static byte UNIQUEID = 5;
    private final static byte DROP = 6;
    private final static byte ASSIGN_ORDINAL = 100;

    private final Map<Integer, InnerTableStatus> tableStatusMap = new HashMap<Integer, InnerTableStatus>(
            INITIAL_MAP_SIZE);
    private final SessionService sessionService;

    static class IncrementRowCount extends UpdateInt {

        IncrementRowCount() {
            super(INCREMENT);
        }

        IncrementRowCount(final int tableId) {
            this();
            _arg = tableId;
        }

        @Override
        public void apply(final TransactionalCache tc) {
            PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg);
            ts.rowWritten();
        }

        @Override
        public boolean cancel(final Update update) {
            if (update instanceof DecrementRowCount) {
                DecrementRowCount drc = (DecrementRowCount) update;
                if (drc.getTableId() == _arg) {
                    return true;
                }
            }
            return false;
        }

        int getTableId() {
            return _arg;
        }

        @Override
        public String toString() {
            return String.format("<IncrementRowCount:%d>", _arg);
        }
    }

    static class DecrementRowCount extends UpdateInt {

        DecrementRowCount() {
            super(DECREMENT);
        }

        DecrementRowCount(final int tableId) {
            this();
            _arg = tableId;
        }

        @Override
        public void apply(final TransactionalCache tc) {
            PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg);
            ts.rowDeleted();
        }

        @Override
        public boolean cancel(final Update update) {
            if (update instanceof IncrementRowCount) {
                IncrementRowCount irc = (IncrementRowCount) update;
                if (irc.getTableId() == _arg) {
                    return true;
                }
            }
            return false;
        }

        int getTableId() {
            return _arg;
        }

        @Override
        public String toString() {
            return String.format("<DecrementRowCount:%d>", _arg);
        }
    }

    static class Truncate extends UpdateInt {

        Truncate() {
            super(TRUNCATE);
        }

        Truncate(final int tableId) {
            this();
            _arg = tableId;
        }

        @Override
        public void apply(final TransactionalCache tc) {
            PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg);
            ts.truncate();
        }

        @Override
        public String toString() {
            return String.format("<Truncate:%d>", _arg);
        }
    }
    
    static class Drop extends UpdateInt {
        
        Drop() {
            super(DROP);
        }
        
        Drop(final int tableId) {
            this();
            _arg = tableId;
        }
        
        @Override
        public void apply(final TransactionalCache tc) {
            PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            tsc.tableStatusMap.remove(_arg);
        }

        @Override
        public String toString() {
            return String.format("<" +
            		"<Drop:%d>", _arg);
        }
    }

    static class AutoIncrementUpdate extends UpdateIntLong {

        AutoIncrementUpdate() {
            super(AUTO_INCREMENT);
        }

        AutoIncrementUpdate(final int tableId, final long value) {
            this();
            this._arg1 = tableId;
            this._arg2 = value;
        }

        @Override
        protected void apply(TransactionalCache tc) {
            final PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg1);
            ts.setAutoIncrement(_arg2);
        }

        @Override
        protected boolean combine(final Update update) {
            if (update instanceof AutoIncrementUpdate) {
                AutoIncrementUpdate au = (AutoIncrementUpdate) update;
                if (au._arg1 == _arg1) {
                    if (_arg2 > au._arg2) {
                        au._arg2 = _arg2;
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return String
                    .format("<UpdateAutoIncrement:%d(%d)>", _arg1, _arg2);
        }
    }

    static class UniqueIdUpdate extends UpdateIntLong {

        UniqueIdUpdate() {
            super(UNIQUEID);
        }

        UniqueIdUpdate(final int tableId, final long value) {
            this();
            this._arg1 = tableId;
            this._arg2 = value;
        }

        @Override
        protected void apply(TransactionalCache tc) {
            final PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg1);
            ts.setUniqueID(_arg2);
        }

        @Override
        protected boolean combine(final Update update) {
            if (update instanceof UniqueIdUpdate) {
                UniqueIdUpdate uiu = (UniqueIdUpdate) update;
                if (uiu._arg1 == _arg1) {
                    if (_arg2 > uiu._arg2) {
                        uiu._arg2 = _arg2;
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("<UniqueIdUpdate:%d(%d)>", _arg1, _arg2);
        }
    }

    static class AssignOrdinalUpdate extends UpdateIntLong {

        AssignOrdinalUpdate() {
            super(ASSIGN_ORDINAL);
        }

        AssignOrdinalUpdate(final int tableId, final long value) {
            this();
            this._arg1 = tableId;
            this._arg2 = value;
        }

        @Override
        protected void apply(TransactionalCache tc) {
            final PersistitTransactionalCacheTableStatusCache tsc = (PersistitTransactionalCacheTableStatusCache) tc;
            final InnerTableStatus ts = tsc.getInternalTableStatus(_arg1);
            ts.setOrdinal((int) _arg2);
        }

        @Override
        public String toString() {
            return String.format("<UniqueIdUpdate:%d(%d)>", _arg1, _arg2);
        }
    }

    @Override
    protected Update createUpdate(byte opCode) {
        switch (opCode) {
        case INCREMENT:
            return new IncrementRowCount();
        case DECREMENT:
            return new DecrementRowCount();
        case TRUNCATE:
            return new Truncate();
        case AUTO_INCREMENT:
            return new AutoIncrementUpdate();
        case UNIQUEID:
            return new UniqueIdUpdate();
        case DROP:
            return new Drop();
        case ASSIGN_ORDINAL:
            return new AssignOrdinalUpdate();

        default:
            throw new IllegalArgumentException("Invalid opCode: " + opCode);
        }
    }

    @Override
    protected long cacheId() {
        return serialVersionUID;
    }

    @Override
    public PersistitTransactionalCacheTableStatusCache copy() {
        return new PersistitTransactionalCacheTableStatusCache(this);
    }

    @Override
    public void save() throws PersistitException {
        final Session session = sessionService.createSession();
        try {
            removeAll(session);
            for (final InnerTableStatus ts : tableStatusMap.values()) {
                final RowDef rowDef = ts.getRowDef();
                if (rowDef != null) {
                    final TreeLink link = treeService.treeLink(
                            rowDef.getSchemaName(), STATUS_TREE_NAME);
                    final Exchange exchange = treeService
                            .getExchange(session, link);
                    try {
                        final int tableId = treeService.aisToStore(link,
                                rowDef.getRowDefId());
                        exchange.clear().append(tableId);
                        ts.put(exchange.getValue());
                        exchange.store();
                    } finally {
                        treeService.releaseExchange(session, exchange);
                    }
                }
            }
        } finally {
             session.close();
        }
    }

    @Override
    public void load() throws PersistitException {
        final Session session = sessionService.createSession();
        try {
            treeService.visitStorage(session, new TreeVisitor() {

                @Override
                public void visit(Exchange exchange) throws PersistitException {
                    loadOneVolume(exchange);
                }

            }, STATUS_TREE_NAME);
        } finally {
            session.close();
        }
    }

    public void loadOneVolume(final Exchange exchange) throws PersistitException {
        exchange.append(Key.BEFORE);
        while (exchange.next()) {
            final int storedTableId = exchange.getKey().reset().decodeInt();
            int tableId = treeService.storeToAis(exchange.getVolume(),
                    storedTableId);
            if (exchange.getValue().isDefined()) {
                InnerTableStatus ts = tableStatusMap.get(tableId);
                if (ts != null && ts.getCreationTime() != 0) {
                    throw new IllegalStateException("TableID " + tableId
                            + " already loaded");
                }
                ts = new InnerTableStatus(tableId);
                ts.get(exchange.getValue());
                tableStatusMap.put(tableId, ts);
            }
        }
    }

    private void removeAll(final Session session) throws PersistitException {
        treeService.visitStorage(session, new TreeVisitor() {

            @Override
            public void visit(Exchange exchange) throws PersistitException  {
                exchange.removeAll();
            }

        }, STATUS_TREE_NAME);
    }

    // ====
    // Public API methods of this TableStatusCache.

    @Override
    public void rowWritten(final int tableId) {
        update(new IncrementRowCount(tableId));
    }

    @Override
    public void rowDeleted(final int tableId) {
        update(new DecrementRowCount(tableId));
    }
    
    @Override
    public void rowUpdated(final int tableId) {
        getInternalTableStatus(tableId).rowUpdated();
    }

    @Override
    public void truncate(final int tableId) {
        update(new Truncate(tableId));
    }
    
    @Override
    public void drop(final int tableId) {
        update(new Drop(tableId));
    }

    @Override
    public void setAutoIncrement(final int tableId, final long value) {
        update(new AutoIncrementUpdate(tableId, value));
    }

    @Override
    public synchronized long createNewUniqueID(int tableID) {
        long id = getInternalTableStatus(tableID).createNewUniqueID();
        setUniqueID(tableID, id);
        return id;
    }

    @Override
    public void setUniqueID(final int tableId, final long value) {
        update(new UniqueIdUpdate(tableId, value));
    }

    @Override
    public void setOrdinal(final int tableId, final int ordinal) {
        update(new AssignOrdinalUpdate(tableId, ordinal));
    }

    @Override
    public synchronized TableStatus getTableStatus(final int tableId) {
        return getInternalTableStatus(tableId);
    }
    
    @Override
    public synchronized void detachAIS() {
        PersistitTransactionalCacheTableStatusCache tsc = this;
        while (tsc != null) {
            for (final InnerTableStatus ts : tsc.tableStatusMap.values()) {
                ts.setRowDef(null);
            }
            tsc = (PersistitTransactionalCacheTableStatusCache)tsc._previousVersion;
        }
    }

    @Override
    public String toString() {
        return String.format("TableStatusCache %s", tableStatusMap);
    }

    private synchronized InnerTableStatus getInternalTableStatus(final int tableId) {
        InnerTableStatus ts = tableStatusMap.get(Integer.valueOf(tableId));
        if (ts == null) {
            ts = new InnerTableStatus(tableId);
            tableStatusMap.put(Integer.valueOf(tableId), ts);
        }
        return ts;
    }

    /**
     * The values maintained in this class are updated frequently. Recording them
     * through the normal Transaction commit mechanism would cause severe
     * performance problems due to high contention. Therefore this class requires
     * special handling to ensure recoverability in the event of an abrupt
     * termination of the server.
     * <p>
     * The purpose of recovery processing in general is to restore the backing
     * B-Trees to an internally consistent, valid state, and then to apply any
     * transactions that had been committed prior to the failure. With respect to
     * TableStatus, the purpose is to restore the row count, unique ID and
     * auto-increment fields to a state that is consistent with those recovered
     * transactions. For example, the row count must accurately reflect the count of
     * all rows inserted into and removed from the database by committed
     * transactions.
     * <p>
     * To accomplish this, TableStatus values are periodically stored on disk using
     * the Persistit Checkpoint mechanism. This event is carefully managed in such a
     * way that all transactions having commit timestamps less than the checkpoint
     * of record, and none committed after that checkpoint, are reflected in the
     * stored record.
     * <p>
     * Upon recovery, Persistit selects a checkpoint to which all B-Trees will be
     * recovered - the recovery checkpoint. The selected checkpoint is determined by
     * finding the last valid checkpoint record in the Journal. Persistit starts its
     * recovery processing by restoring the values of all pages in all B-trees to
     * their pre-checkpoint states.
     * <p>
     * Next Persistit recovers all transactions that were committed after the
     * recovery checkpoint. While applying these transactions, TableStatus values
     * are updated to adjust the row count, unique ID and auto-increment fields
     * according to the transactions that committed after the checkpoint.
     *
     * @author peter
     */
    private static class InnerTableStatus implements TableStatus {
        private static final int SERIALIZATION_VERSION = 100;

        private boolean dirty;
        private final int tableId;
        private RowDef currentRowDef;

        private int ordinal;
        private long rowCount;
        private long uniqueIdValue;
        private long autoIncrementValue;

        private boolean isAutoIncrement;
        private long creationTime;
        private long lastDeleteTime;
        private long lastReadTime;
        private long lastUpdateTime;
        private long lastWriteTime;

        public InnerTableStatus(final int tableId) {
            this.tableId = tableId;
            this.creationTime = now();
        }

        public InnerTableStatus(final InnerTableStatus ts) {
            this.tableId = ts.tableId;
            this.currentRowDef = ts.currentRowDef;
            this.ordinal = ts.ordinal;
            this.isAutoIncrement = ts.isAutoIncrement;
            this.autoIncrementValue = ts.autoIncrementValue;
            this.uniqueIdValue = ts.uniqueIdValue;
            this.rowCount = ts.rowCount;
            this.creationTime = ts.creationTime;
            this.lastDeleteTime = ts.lastDeleteTime;
            this.lastReadTime = ts.lastReadTime;
            this.lastUpdateTime = ts.lastUpdateTime;
            this.lastWriteTime = ts.lastWriteTime;
            this.dirty = ts.dirty;
        }

        @Override
        public synchronized String toString() {
            return String.format("TableStatus(rowDefId=%d,ordinal=%d,isAutoIncrement=%s,autoIncrementValue=%d,rowCount=%d)",
                                 currentRowDef.getRowDefId(), ordinal, isAutoIncrement, autoIncrementValue, rowCount);
        }

        //
        // TableStatus
        //

        @Override
        public synchronized long getAutoIncrement() {
            return autoIncrementValue;
        }

        @Override
        public synchronized long getCreationTime() {
            return creationTime;
        }

        @Override
        public synchronized long getLastDeleteTime() {
            return lastDeleteTime;
        }

        @Override
        public synchronized long getLastUpdateTime() {
            return lastUpdateTime;
        }

        @Override
        public synchronized long getLastWriteTime() {
            return lastWriteTime;
        }

        @Override
        public synchronized int getOrdinal() {
            return ordinal;
        }

        @Override
        public synchronized long getRowCount() {
            return rowCount;
        }

        @Override
        public synchronized long getUniqueID() {
            return uniqueIdValue;
        }

        @Override
        public synchronized RowDef getRowDef() {
            return currentRowDef;
        }

        @Override
        public synchronized void setRowDef(final RowDef rowDef) {
            this.currentRowDef = rowDef;
            dirty = true;
        }

        //
        // Internal
        //

        private static long now() {
            return System.currentTimeMillis();
        }

        synchronized void rowDeleted() {
            rowCount = Math.max(0, rowCount - 1);
            lastDeleteTime = now();
            dirty = true;
        }

        synchronized void rowUpdated() {
            lastUpdateTime = now();
            dirty = true;
        }

        synchronized void rowWritten() {
            ++rowCount;
            lastWriteTime = now();
            dirty = true;
        }

        synchronized void setAutoIncrement(long autoIncrement) {
            this.autoIncrementValue = Math.max(this.autoIncrementValue, autoIncrement);
            dirty = true;
        }

        synchronized void setOrdinal(int ordinal) {
            if (this.ordinal != ordinal) {
                this.ordinal = ordinal;
                dirty = true;
            }
        }

        synchronized void setUniqueID(long uniqueId) {
            this.uniqueIdValue = Math.max(uniqueIdValue, uniqueId);
            dirty = true;
        }

        synchronized long createNewUniqueID() {
            return ++uniqueIdValue;
        }

        synchronized void truncate() {
            rowCount = 0;
            autoIncrementValue = 0;
            uniqueIdValue = 0;
            lastDeleteTime = now();
            dirty = true;
        }

        /**
         * Serialize this table status into the supplied Persistit Value object.
         * This eliminates the need to have a custom PersistitValueCoder and
         * therefore simplifies managing the saved state of the system.
         *
         * @param value Destination of serialized state
         * @throws com.persistit.exception.ConversionException For a failure during serialization
         */
        void put(Value value) throws ConversionException {
            if (creationTime == 0) {
                creationTime = now();
            }
            value.setStreamMode(true);
            try {
                value.put(SERIALIZATION_VERSION);
                value.put(currentRowDef.getSchemaName());
                value.put(currentRowDef.getTableName());
                value.put(currentRowDef.getRowDefId());
                value.put(ordinal);
                value.put(isAutoIncrement);
                value.put(autoIncrementValue);
                value.put(uniqueIdValue);
                value.put(rowCount);
                value.put(creationTime);
                value.put(lastWriteTime);
                value.put(lastReadTime);
                value.put(lastUpdateTime);
                value.put(lastDeleteTime);
            } finally {
                value.setStreamMode(false);
            }
        }

        /**
         * Deserialize this table status from the supplied Persistit Value object.
         *
         * @param value Source of serialized state
         */
        void get(Value value) {
            value.setStreamMode(true);
            try {
                final long version = value.getInt();
                if (version != SERIALIZATION_VERSION) {
                    throw new IllegalArgumentException("Can't decode table status written as version " + version);
                }

                /*
                * schemaName and tableName are stored for human readability- code
                * does not need these strings.
                */
                final String schemaName = value.getString();
                final String tableName = value.getString();

                final int rowDefId = value.getInt();
                if (currentRowDef != null && rowDefId != currentRowDef.getRowDefId()) {
                    throw new IllegalArgumentException("Can't decode " + this + " from record for rowDefId=" + rowDefId);
                }
                ordinal = value.getInt();
                isAutoIncrement = value.getBoolean();
                autoIncrementValue = value.getLong();
                setUniqueID(value.getLong());
                rowCount = value.getLong();
                creationTime = value.getLong();
                lastWriteTime = value.getLong();
                lastReadTime = value.getLong();
                lastUpdateTime = value.getLong();
                lastDeleteTime = value.getLong();
            } finally {
                value.setStreamMode(false);
            }
        }
    }
}
