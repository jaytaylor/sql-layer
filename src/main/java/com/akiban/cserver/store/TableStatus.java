package com.akiban.cserver.store;

import com.persistit.Value;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.ValueRenderer;
import com.persistit.exception.ConversionException;

import java.io.Serializable;

import static com.akiban.cserver.store.PersistitStoreTableManager.now;

/**
 * Structure denotes information about a table. Flushed to backing store
 * periodically, and transactionally for specific fields (ordinal, isDeleted).
 * 
 * @author peter
 * 
 */
public class TableStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final long MAX_STALE_TIME = 30000L;

    private final int rowDefId;

    private int ordinal;

    private boolean isDeleted;

    private boolean isAutoIncrement;

    private long autoIncrementValue;

    private long rowCount;

    private long creationTime;

    private long lastWriteTime;

    private long lastReadTime;

    private long lastUpdateTime;

    private long lastDeleteTime;

    private long lastFlushed;

    private boolean dirty;

    public void reset() {
        ordinal = 0;
        isDeleted = false;
        isAutoIncrement = false;
        autoIncrementValue = 0;
        rowCount = 0;
        creationTime = 0;
        lastWriteTime = 0;
        lastReadTime = 0;
        lastUpdateTime = 0;
        lastDeleteTime = 0;
        lastFlushed = 0;
        dirty = false;
    }

    public TableStatus(final int rowDefId) {
        this.rowDefId = rowDefId;
        reset();
    }

    public synchronized int getRowDefId() {
        return rowDefId;
    }

    public synchronized int getOrdinal() {
        return ordinal;
    }

    public synchronized void setOrdinal(int ordinal) {
        if (this.ordinal != 0) {
            throw new IllegalStateException("Attempt to reassign " + this
                    + " ordinal to " + ordinal);
        }
        if (this.ordinal != ordinal) {
            this.ordinal = ordinal;
            dirty = true;
        }
    }

    public synchronized boolean isDeleted() {
        return isDeleted;
    }

    public synchronized void setDeleted(boolean isDeleted) {
        if (this.isDeleted && !isDeleted) {
            throw new IllegalStateException("Attempt to undelete " + this);
        }
        if (this.isDeleted != isDeleted) {
            this.isDeleted = isDeleted;
            dirty = true;
        }
    }

    public synchronized boolean isAutoIncrement() {
        return isAutoIncrement;
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

    public synchronized boolean testIsStale() {
        final long now = System.nanoTime() / 1000;
        if (dirty && now - lastFlushed > MAX_STALE_TIME) {
            lastFlushed = now;
            return true;
        } else {
            return false;
        }
    }

    public synchronized void flushed() {
        dirty = false;
    }

    public synchronized String toString() {
        return String.format("TableStatus(rowDefId=%d,ordinal=%d,isDeleted=%s,"
                + "isAutoIncrement=%s,autoIncrementValue=%d,rowCount=%d)",
                rowDefId, ordinal, isDeleted, isAutoIncrement,
                autoIncrementValue, rowCount);
    }

    static class PersistitEncoder implements ValueRenderer {

        @Override
        public void put(Value value, Object object, CoderContext context)
                throws ConversionException {
            final TableStatus ts = (TableStatus) object;
            if (ts.creationTime == 0) {
                ts.creationTime = now();
            }
            value.put(ts.rowDefId);
            value.put(ts.ordinal);
            value.put(ts.isDeleted);
            value.put(ts.isAutoIncrement);
            value.put(ts.autoIncrementValue);
            value.put(ts.rowCount);
            value.put(ts.creationTime);
            value.put(ts.lastWriteTime);
            value.put(ts.lastReadTime);
            value.put(ts.lastUpdateTime);
            value.put(ts.lastDeleteTime);
        }

        @Override
        public void render(Value value, Object target, Class clazz,
                CoderContext context) throws ConversionException {
            final TableStatus ts = (TableStatus) target;
            final int rowDefId = value.getInt();
            final int ordinal = value.getInt();
            final boolean isDeleted = value.getBoolean();
            final boolean isAutoIncrement = value.getBoolean();
            final long autoIncrementValue = value.getLong();
            final long rowCount = value.getLong();
            final long creationTime = value.getLong();
            final long lastWriteTime = value.getLong();
            final long lastReadTime = value.getLong();
            final long lastUpdateTime = value.getLong();
            final long lastDeleteTime = value.getLong();
            //
            // Test some invariants
            //
            if (ts.rowDefId != rowDefId) {
                throw new ConversionException(
                        "Attempt to reassign RowDefId to " + rowDefId + " on "
                                + ts);
            }

            if (ts.ordinal != 0 && ts.ordinal != ordinal) {
                throw new ConversionException("Attempt to reassign ordinal to "
                        + ordinal + " on " + ts);
            }

            if (ts.isDeleted && !isDeleted) {
                throw new ConversionException(
                        "Attempt to set isDelete to false on " + ts);
            }

            if (ts.isAutoIncrement && !isAutoIncrement) {
                throw new ConversionException(
                        "Attempt to set isAutoIncrement to false on " + ts);
            }

            ts.ordinal = ordinal;
            ts.isDeleted = isDeleted;
            ts.isAutoIncrement = isAutoIncrement;
            ts.autoIncrementValue = autoIncrementValue;
            ts.rowCount = rowCount;
            ts.creationTime = creationTime;
            ts.lastWriteTime = lastWriteTime;
            ts.lastReadTime = lastReadTime;
            ts.lastUpdateTime = lastUpdateTime;
            ts.lastDeleteTime = lastDeleteTime;
        }

        @Override
        public Object get(Value value, Class clazz, CoderContext context)
                throws ConversionException {
            final int rowDefId = ((Integer) value.peek()).intValue();
            final TableStatus tableStatus = new TableStatus(rowDefId);
            render(value, tableStatus, TableStatus.class, null);
            return tableStatus;
        }
    }
}
