package com.akiban.cserver.service.tree;

import com.akiban.cserver.StorageLink;
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
public interface TreeService extends Service<TreeService> {

    static String VOLUME_NAME = "akiban_data";

    static String SCHEMA_TREE_NAME = "_schema_";

    static String BY_ID = "byId";

    static String BY_NAME = "byName";

    Persistit getDb();

    Exchange getExchange(Session session, StorageLink context)
            throws PersistitException;

    Exchange getExchange(Session session, Tree tree) throws PersistitException;
    
    void releaseExchange(Session session, Exchange exchange);

    Transaction getTransaction(Session session);

    void visitStorage(Session session, TreeVisitor visitor, String treeName)
            throws Exception;

    int volumeHandle(Exchange exchange);

    long getTimestamp(Session session);

    boolean isContainer(Exchange exchange, StorageLink storageLink);

}
