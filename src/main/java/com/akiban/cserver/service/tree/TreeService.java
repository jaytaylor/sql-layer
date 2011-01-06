package com.akiban.cserver.service.tree;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface TreeService extends Service<TreeService> {

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String STATUS_TREE_NAME = "_status_";

    final static String TREESPACE="treespace";
    
    final static String SCHEMA = "schema";
    
    final static String TREE = "tree";

    Persistit getDb();

    Exchange getExchange(Session session, TreeLink context)
            throws PersistitException;

    Exchange getExchange(Session session, Tree tree) throws PersistitException;

    void releaseExchange(Session session, Exchange exchange);

    Transaction getTransaction(Session session);

    void visitStorage(Session session, TreeVisitor visitor, String treeName)
            throws Exception;

    long getTimestamp(Session session);

    boolean isContainer(Exchange exchange, TreeLink storageLink) throws PersistitException;

    int aisToStore(final TreeLink link, final int logicalTableId)
            throws PersistitException;

    int storeToAis(final TreeLink link, final int storedTableId)
            throws PersistitException;

    int storeToAis(final Volume volume, final int storedTableId)
            throws PersistitException;
}
