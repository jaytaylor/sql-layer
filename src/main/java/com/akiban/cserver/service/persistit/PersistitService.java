package com.akiban.cserver.service.persistit;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface PersistitService extends Service<PersistitService> {
    
    final static String VOLUME_NAME = "akiban_data";
    
    final static String SCHEMA_TREE_NAME = "_schema_";
    
    final static String BY_ID = "byId";

    final static String BY_NAME = "byName";

    Persistit getDb();
    
    Exchange getExchange(final Session session, final String schemaName, final String tableName) throws PersistitException;
    
    Exchange getExchange(final Session session, final Tree tree) throws PersistitException;
    
    void releaseExchange(final Session session, final Exchange exchange);
    
    Transaction getTransaction();
    
    void visitStorage(StorageVisitor visitor, final String treeName, final Object object) throws PersistitException;
    
    int volumeHandle(final Exchange exchange);
    
}
