package com.akiban.server;

public interface TableStatusCache {
    void rowDeleted(int tableID);

    void rowUpdated(int tableID);

    void rowWritten(int tableID);

    void truncate(int tableID);

    void drop(int tableID);

    void setAutoIncrement(int tableID, long value);

    long createNewUniqueID(int tableID);
    void setUniqueID(int tableID, long value);

    void setOrdinal(int tableID, int value);

    TableStatus getTableStatus(int tableID);

    void detachAIS();
}
