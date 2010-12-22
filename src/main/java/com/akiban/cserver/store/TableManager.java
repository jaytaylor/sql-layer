package com.akiban.cserver.store;

import com.akiban.cserver.service.session.Session;
import com.persistit.exception.PersistitException;

public interface TableManager {

    public abstract TableStatus getTableStatus(Session session, int rowDefId)
            throws PersistitException;

    public abstract void loadStatus(Session session, TableStatus tableStatus)
            throws PersistitException;

    public abstract void saveStatus(Session session, TableStatus tableStatus)
            throws PersistitException;

    public abstract void deleteStatus(Session session, TableStatus tableStatus)
            throws PersistitException;

    public abstract void deleteStatus(Session session, int rowDefId)
            throws PersistitException;

}