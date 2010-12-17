package com.akiban.cserver.store;

import com.persistit.exception.PersistitException;

public interface TableManager {

    public abstract TableStatus getTableStatus(final int rowDefId)
            throws PersistitException;

    public abstract void loadStatus(final TableStatus tableStatus)
            throws PersistitException;

    public abstract void saveStatus(final TableStatus tableStatus)
            throws PersistitException;

    public abstract void deleteStatus(final TableStatus tableStatus)
            throws PersistitException;

    public abstract void deleteStatus(final int rowDefId)
            throws PersistitException;

}