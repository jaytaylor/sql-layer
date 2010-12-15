package com.akiban.cserver.store;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface PersistitService extends Service<PersistitService> {

    Persistit getDb();
    
    Exchange getExchange(final Session session, final String schemaName, final String tableName) throws Exception;
    
    void releaseExchange(final Session sesison, final Exchange exchange);
    
    void visitStorage(StorageVisitor visitor, final String treeName, final Object object) throws Exception;
    
}
