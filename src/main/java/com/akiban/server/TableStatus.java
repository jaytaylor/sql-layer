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

import com.akiban.server.rowdata.RowDef;
import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other. When in the future we implement
 * schema evolution in which a new RowDef instance with a new tableId replaces
 * the old one, there will be just one TableStatus referenced by all the RowDef
 * versions, and the TableStatus will refer back to just the newest RowDef.
 * Hence intended relationship between TableStatus and RowDef is one-to-many.
 * <p>
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
 * 
 */
public class TableStatus {

    private static final int VERSION = 100;

    private final int tableId;

    private volatile RowDef currentRowDef;

    private volatile int ordinal;

    // Used for pk-less tables
    // This is a counter used to create new values.
    private volatile long uniqueIdCounter;

    private boolean isAutoIncrement;

    // The largest autoIncrementValue found in a committed
    // writeRow operation
    private volatile long autoIncrementValue;

    // This is the maximum of all values actually committed
    // This value, rather than uniqueIdCounter, is what
    // gets serialized.
    //
    private volatile long uniqueIdValue;

    private volatile long rowCount;

    private volatile long creationTime;

    private volatile long lastWriteTime;

    private volatile long lastReadTime;

    private volatile long lastUpdateTime;

    private volatile long lastDeleteTime;

    private volatile long timestamp;
    
    private volatile boolean dirty;

    private static long now() {
        return System.currentTimeMillis();
    }

    public TableStatus(final int tableId) {
        this.tableId = tableId;
        this.creationTime = now();
    }
    
    public TableStatus(final TableStatus ts) {
        this.tableId = ts.tableId;
        this.currentRowDef = ts.currentRowDef;
        this.ordinal = ts.ordinal;
        this.isAutoIncrement = ts.isAutoIncrement;
        this.autoIncrementValue = ts.autoIncrementValue;
        this.uniqueIdCounter = ts.uniqueIdCounter;
        this.uniqueIdValue = ts.uniqueIdValue;
        this.rowCount = ts.rowCount;
        this.creationTime = ts.creationTime;
        this.lastDeleteTime = ts.lastDeleteTime;
        this.lastReadTime = ts.lastReadTime;
        this.lastUpdateTime = ts.lastUpdateTime;
        this.lastWriteTime = ts.lastWriteTime;
        this.timestamp = ts.timestamp;
        this.dirty = ts.dirty;
    }

    public synchronized RowDef getRowDef() {
        return currentRowDef;
    }
    
    public synchronized int getRowDefId() {
        return currentRowDef.getRowDefId();
    }

    public synchronized int getOrdinal() {
        return ordinal;
    }

    public synchronized boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public synchronized boolean isDirty() {
        return dirty;
    }

    public synchronized long getAutoIncrementValue() {
        return autoIncrementValue;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public synchronized long getLastWriteTime() {
        return lastWriteTime;
    }

    public synchronized long getLastReadTime() {
        return lastReadTime;
    }

    public synchronized long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public synchronized long getLastDeleteTime() {
        return lastDeleteTime;
    }

    public synchronized long getRowCount() {
        return rowCount;
    }

    // ----------

    public synchronized void setRowDef(final RowDef rowDef) {
        this.currentRowDef = rowDef;
        dirty = true;
    }

    public synchronized void setOrdinal(int ordinal) {
        if (this.ordinal != ordinal) {
            this.ordinal = ordinal;
            dirty = true;
        }
    }
    
    public synchronized void setAutoIncrement(final boolean autoIncrement) {
        this.isAutoIncrement = autoIncrement;
    }

    public synchronized void updateAutoIncrementValue(long autoIncrementValue) {
        this.autoIncrementValue = Math.max(this.autoIncrementValue,
                autoIncrementValue);
        dirty = true;
    }

    public synchronized void updateUniqueIdValue(long uniqueId) {
        this.uniqueIdValue = Math.max(this.uniqueIdValue, uniqueId);
        if (uniqueIdValue > uniqueIdCounter) {
            uniqueIdCounter = uniqueIdValue;
        }
        dirty = true;
    }

    /**
     * Invoked by truncate
     */
    public synchronized void truncate() {
        rowCount = 0;
        autoIncrementValue = 0;
        uniqueIdCounter = 0;
        uniqueIdValue = 0;
        updateDeleteTime();
        dirty = true;
    }
    
    public synchronized void drop() {
        truncate();
        isAutoIncrement = false;
        ordinal = 0;
        creationTime = 0;
        lastDeleteTime = 0;
        lastReadTime = 0;
        lastUpdateTime = 0;
        lastWriteTime = 0;
        dirty = true;
    }
    
    public synchronized void incrementRowCount(long delta) {
        this.rowCount = Math.max(0, this.rowCount + delta);
        if (delta > 0) {
            updateWriteTime();
        } else {
            updateDeleteTime();
        }
        dirty = true;
    }

    public synchronized void updateWriteTime() {
        this.lastWriteTime = now();
        dirty = true;
    }

    public synchronized void updateReadTime() {
        this.lastReadTime = now();
        dirty = true;
    }

    public synchronized void updateUpdateTime() {
        this.lastUpdateTime = now();
        dirty = true;
    }

    public synchronized void updateDeleteTime() {
        this.lastDeleteTime = now();
        dirty = true;
    }

    // ------------------

    public synchronized long allocateNewUniqueId() {
        return ++uniqueIdCounter;
    }

    public synchronized String toString() {
        return String.format("TableStatus(rowDefId=%d,ordinal=%d,"
                + "isAutoIncrement=%s,autoIncrementValue=%d,rowCount=%d)",
                currentRowDef.getRowDefId(), ordinal, isAutoIncrement,
                autoIncrementValue, rowCount);
    }

    /**
     * Serialize this table status into the supplied Persistit Value object.
     * This eliminates the need to have a custom PersistitValueCoder and
     * therefore simplifies managing the saved state of the system.
     * 
     * @param value
     * @throws ConversionException
     */
    public synchronized void put(Value value)
            throws ConversionException {
        if (creationTime == 0) {
            creationTime = now();
        }
        value.setStreamMode(true);
        try {
            value.put(VERSION);
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
            value.put(timestamp);
        } finally {
            value.setStreamMode(false);
        }
    }

    /**
     * Deserialize this table status from the supplied Persistit Value object.
     * 
     * @param value
     */
    public void get(Value value) {
        value.setStreamMode(true);
        try {
            final long version = value.getInt();
            if (version != VERSION) {

                throw new IllegalArgumentException(
                        "Can't decode table status written as version "
                                + version);
            }

            /*
             * schemaName and tableName are stored for human readability- code
             * does not need these strings.
             */
            final String schemaName = value.getString();
            final String tableName = value.getString();

            final int rowDefId = value.getInt();
            if (currentRowDef != null && rowDefId != currentRowDef.getRowDefId()) {
                throw new IllegalArgumentException("Can't decode " + this
                        + " from record for rowDefId=" + rowDefId);
            }
            ordinal = value.getInt();
            isAutoIncrement = value.getBoolean();
            autoIncrementValue = value.getLong();
            uniqueIdValue = value.getLong();
            rowCount = value.getLong();
            creationTime = value.getLong();
            lastWriteTime = value.getLong();
            lastReadTime = value.getLong();
            lastUpdateTime = value.getLong();
            lastDeleteTime = value.getLong();
            timestamp = value.getLong();
        } finally {
            value.setStreamMode(false);
        }
        updateUniqueIdValue(uniqueIdCounter);
    }

}
