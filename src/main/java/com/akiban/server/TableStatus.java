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

import com.persistit.Value;
import com.persistit.exception.ConversionException;

/**
 * Structure denotes information about a table. Flushed to backing store
 * periodically, and transactionally for specific fields (ordinal, isDeleted).
 * 
 * @author peter
 * 
 */
public class TableStatus {

    private static final int VERSION = 100;

    private static final int MAX_STALE_TIME = 5000;

    private final RowDef rowDef;

    private int ordinal;

    private boolean isAutoIncrement;

    private long autoIncrementValue;
    
    private long uniqueId; // Used for pk-less tables

    private long rowCount;

    private long creationTime;

    private long lastWriteTime;

    private long lastReadTime;

    private long lastUpdateTime;

    private long lastDeleteTime;

    private long lastFlushed;

    private long timestamp;

    private boolean dirty;

    private static long now() {
        return System.currentTimeMillis();
    }

    public TableStatus(final RowDef rowDef) {
        this.rowDef = rowDef;
        dirty = true;
    }

    public synchronized int getRowDefId() {
        return rowDef.getRowDefId();
    }

    public synchronized int getOrdinal() {
        return ordinal;
    }

    public synchronized void setOrdinal(int ordinal) {
        if (this.ordinal != ordinal) {
            this.ordinal = ordinal;
            dirty = true;
        }
    }

    public synchronized boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public synchronized boolean isDirty() {
        return dirty;
    }

    public synchronized void setAutoIncrement(boolean isAutoIncrement) {
        if (this.isAutoIncrement != isAutoIncrement) {
            this.isAutoIncrement = isAutoIncrement;
            dirty = true;
        }
    }

    public synchronized long getAutoIncrementValue() {
        return autoIncrementValue;
    }

    public synchronized void setAutoIncrementValue(long autoIncrementValue) {
        if (this.autoIncrementValue != autoIncrementValue) {
            this.autoIncrementValue = autoIncrementValue;
            dirty = true;
        }
    }

    public long getCreationTime() {
        return creationTime;
    }

    public synchronized long getLastWriteTime() {
        return lastWriteTime;
    }

    public synchronized void written() {
        this.lastWriteTime = now();
        dirty = true;
    }

    public synchronized long getLastReadTime() {
        return lastReadTime;
    }

    public synchronized void read() {
        this.lastReadTime = now();
        dirty = true;
    }

    public synchronized long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public synchronized void updated() {
        this.lastUpdateTime = now();
        dirty = true;
    }

    public synchronized long getLastDeleteTime() {
        return lastDeleteTime;
    }

    public synchronized void deleted() {
        this.lastDeleteTime = now();
        dirty = true;
    }

    public synchronized long getRowCount() {
        return rowCount;
    }

    public synchronized void setRowCount(final long count) {
        this.rowCount = count;
        dirty = true;
    }

    public synchronized void incrementRowCount(long delta) {
        this.rowCount = Math.max(0, this.rowCount + delta);
        dirty = true;
    }
    
    public synchronized long newUniqueId() {
        dirty = true;
        return uniqueId++;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public synchronized void flushed() {
        dirty = false;
    }

    public synchronized String toString() {
        return String.format("TableStatus(rowDefId=%d,ordinal=%d,"
                + "isAutoIncrement=%s,autoIncrementValue=%d,rowCount=%d)",
                rowDef.getRowDefId(), ordinal, isAutoIncrement,
                autoIncrementValue, rowCount);
    }

    public synchronized boolean testIsStale() {
        final long now = System.nanoTime() / 1000;
        if (dirty && now - lastFlushed > MAX_STALE_TIME) {
            lastFlushed = now;
            return true;
        } else {
            return false;
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
            value.put(rowDef.getSchemaName());
            value.put(rowDef.getTableName());
            value.put(rowDef.getRowDefId());
            value.put(ordinal);
            value.put(isAutoIncrement);
            value.put(autoIncrementValue);
            value.put(uniqueId);
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
                // TODO - this is temporary until we've done the "transactional"
                // table status.
                throw new IllegalArgumentException(
                        "Can't decode table status written as version "
                                + version);
            }
            
            /*
             * schemaName and tableName are stored for human readability-
             * we don't need the strings here.
             */
            final String schemaName = value.getString();
            final String tableName = value.getString();

            final int rowDefId = value.getInt();
            if (rowDefId != rowDef.getRowDefId()) {
                throw new IllegalArgumentException("Can't decode " + this
                        + " from record for rowDefId=" + rowDefId);
            }
            ordinal = value.getInt();
            isAutoIncrement = value.getBoolean();
            autoIncrementValue = value.getLong();
            uniqueId = value.getLong();
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
    }

}
