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

import com.persistit.TimestampAllocator.Checkpoint;
import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other. When in the future we implement
 * schema evolution in which a new RowDef intance with a new tableId replaces
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

    public static final Checkpoint CHECKPOINT_ZERO = new Checkpoint(0, 0);
    
    private static final int VERSION = 100;

    private final int tableId;

    private  RowDef currentRowDef;

    private int ordinal;

    // Used for pk-less tables
    // This is a counter used to create new values.
    private long uniqueIdCounter;

    private boolean isAutoIncrement;

    // The largest autoIncrementValue found in a committed
    // writeRow operation
    private long autoIncrementValue;

    // This is the maximum of all values actually committed
    // This value, rather than uniqueIdCounter, is what
    // gets serialized.
    // 
    private long uniqueIdValue;

    private long rowCount;

    private long creationTime;

    private long lastWriteTime;

    private long lastReadTime;

    private long lastUpdateTime;

    private long lastDeleteTime;

    private long checkpoint = CHECKPOINT_ZERO.getTimestamp();
    
    private boolean deleted = true;

    private boolean dirty;
    
    private TableStatus previous;
    
    private static long now() {
        return System.currentTimeMillis();
    }

    public TableStatus(final int tableId) {
        this.tableId = tableId;
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
    
    public synchronized boolean isDeleted() {
        return deleted;
    }
    
    // ----------
    
    public synchronized void setRowDef(final RowDef rowDef) {
        if (previous != null) {
            previous.setRowDef(rowDef);
        }
        this.currentRowDef = rowDef;
        dirty = true;
    }

    public synchronized void setOrdinal(int ordinal) {
        if (previous != null) {
            previous.setOrdinal(ordinal);
        }
        if (this.ordinal != ordinal) {
            this.ordinal = ordinal;
            dirty = true;
        }
    }

    public synchronized void setAutoIncrementEnabled(boolean isAutoIncrement) {
        if (this.isAutoIncrement != isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
            dirty = true;
        }
    }

    public synchronized void updateAutoIncrementValue(final Checkpoint checkpoint, final long timestamp, long autoIncrementValue) {
        if (split(checkpoint, timestamp)) {
            updateAutoIncrementValue(CHECKPOINT_ZERO, timestamp, autoIncrementValue);
        }
        this.autoIncrementValue = Math.max(this.autoIncrementValue,
                autoIncrementValue);
        dirty = true;
    }

    public synchronized void updateUniqueIdValue(final Checkpoint checkpoint, final long timestamp, long uniqueId) {
        if (split(checkpoint, timestamp)) {
            updateUniqueIdValue(CHECKPOINT_ZERO, timestamp, uniqueId);
        }
        this.uniqueIdValue = Math.max(this.uniqueIdValue, uniqueId);
        dirty = true;
    }

    public synchronized void zeroRowCount(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            zeroRowCount(CHECKPOINT_ZERO, timestamp);
        }
        this.rowCount = 0;
        dirty = true;
    }

    public synchronized void incrementRowCount(final Checkpoint checkpoint, final long timestamp, long delta) {
        if (split(checkpoint, timestamp)) {
            incrementRowCount(CHECKPOINT_ZERO, timestamp, delta);
        }
        this.rowCount = Math.max(0, this.rowCount + delta);
        dirty = true;
    }

    public synchronized void updateWriteTime(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            updateWriteTime(CHECKPOINT_ZERO, timestamp);
        }
        this.lastWriteTime = now();
        dirty = true;
    }

    public synchronized void updateReadTime(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            updateReadTime(CHECKPOINT_ZERO, timestamp);
        }
        this.lastReadTime = now();
        dirty = true;
    }

    public synchronized void updateUpdateTime(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            updateUpdateTime(CHECKPOINT_ZERO, timestamp);
        }
        this.lastUpdateTime = now();
        dirty = true;
    }

    public synchronized void updateDeleteTime(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            updateDeleteTime(CHECKPOINT_ZERO, timestamp);
        }
        this.lastDeleteTime = now();
        dirty = true;
    }
    
    public synchronized void delete(final Checkpoint checkpoint, final long timestamp) {
        if (split(checkpoint, timestamp)) {
            delete(CHECKPOINT_ZERO, timestamp);
        }
        this.deleted = true;
        dirty = true;
    }
   
    // ------------------

    public synchronized long allocateNewUniqueId() {
        return uniqueIdCounter++;
    }

    public synchronized void flushed() {
        dirty = false;
    }

    public synchronized String toString() {
        return String.format("TableStatus(rowDefId=%d,ordinal=%d,"
                + "isAutoIncrement=%s,autoIncrementValue=%d,rowCount=%d)",
                currentRowDef.getRowDefId(), ordinal, isAutoIncrement,
                autoIncrementValue, rowCount);
    }

    private boolean split(final Checkpoint checkpoint, final long timestamp) {
        if (checkpoint.getTimestamp() > this.checkpoint) {
            final TableStatus clone = new TableStatus(tableId);
            clone.autoIncrementValue = this.autoIncrementValue;
            clone.checkpoint = this.checkpoint;
            clone.creationTime = this.creationTime;
            clone.currentRowDef = this.currentRowDef;
            clone.deleted = this.deleted;
            clone.dirty = this.dirty;
            clone.isAutoIncrement = this.isAutoIncrement;
            clone.lastDeleteTime = this.lastDeleteTime;
            clone.lastReadTime = this.lastReadTime;
            clone.lastUpdateTime = this.lastUpdateTime;
            clone.lastWriteTime = this.lastWriteTime;
            clone.ordinal = this.ordinal;
            clone.previous = this.previous;
            clone.rowCount = this.rowCount;
            clone.uniqueIdCounter = this.uniqueIdCounter;
            clone.uniqueIdValue = this.uniqueIdValue;
            this.previous = clone;
        }
        return timestamp < this.checkpoint;
    }

    /**
     * Find and return a version of this TableStatus whose checkpoint less than or
     * equal to the supplied timestamp. If there is no such version then return 
     * <code>null</code>
     * @param the checkpointTimestamp of the desired TableStatus version
     * @return  the TableStatus version, or <code>null</code> if there is none
     */
    public synchronized TableStatus version(final long checkpointTimestamp) {
        TableStatus ts = this;
        while (ts != null) {
            if (ts.checkpoint <= checkpointTimestamp) {
                break;
            }
            ts = ts.previous;
        }
        return ts;
    }
    
    /**
     * Remove any versions having timestamps before or equal to the supplied
     * checkpoint timestamp.  The CheckpointListener does this after writing
     * the records to the _status_ tree.
     * @param checkpointTimestamp
     */
    public synchronized void pruneObsoleteVersions(final long checkpointTimestamp) {
        TableStatus ts = this;
        while (ts.previous != null) {
            if (ts.previous.checkpoint <= checkpointTimestamp) {
                ts.previous = null;
                break;
            }
            ts = ts.previous;
        }
    }
    /**
     * Serialize this table status into the supplied Persistit Value object.
     * This eliminates the need to have a custom PersistitValueCoder and
     * therefore simplifies managing the saved state of the system.
     * 
     * @param value
     * @throws ConversionException
     */
    public void put(Value value) throws ConversionException {
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
            value.put(checkpoint);
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
            if (rowDefId != currentRowDef.getRowDefId()) {
                throw new IllegalArgumentException("Can't decode " + this
                        + " from record for rowDefId=" + rowDefId);
            }
            ordinal = value.getInt();
            isAutoIncrement = value.getBoolean();
            autoIncrementValue = value.getLong();
            uniqueIdCounter = value.getLong();
            rowCount = value.getLong();
            creationTime = value.getLong();
            lastWriteTime = value.getLong();
            lastReadTime = value.getLong();
            lastUpdateTime = value.getLong();
            lastDeleteTime = value.getLong();
            checkpoint = value.getLong();
        } finally {
            value.setStreamMode(false);
        }
    }

}
