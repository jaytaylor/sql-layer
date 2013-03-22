
package com.akiban.server.service.tree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.TableStatusCache;
import com.akiban.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Key;
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
public interface TreeService extends KeyCreator {

    final static Logger logger = LoggerFactory.getLogger(TreeService.class.getName());

    final static int MAX_TABLES_PER_VOLUME = 100000;

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String TREESPACE = "treespace";

    final static String SCHEMA = "schema";

    final static String TREE = "tree";

    Persistit getDb();

    Exchange getExchange(Session session, TreeLink context);

    Exchange getExchange(Session session, Tree tree);

    Key getKey();

    void releaseExchange(Session session, Exchange exchange);

    /** @deprecated Use the TransactionService */
    Transaction getTransaction(Session session);

    void visitStorage(Session session, TreeVisitor visitor, String treeName) throws PersistitException;

    boolean isContainer(Exchange exchange, TreeLink storageLink);

    int aisToStore(final TreeLink link, final int logicalTableId);

    int storeToAis(final TreeLink link, final int storedTableId);

    int storeToAis(final Volume volume, final int storedTableId);

    void checkpoint() throws PersistitException;

    TableStatusCache getTableStatusCache();

    TreeLink treeLink(final String schemaName, final String treeName);

    String getDataPath();

    String volumeForTree(final String schemaName, final String treeName);

    boolean treeExists(final String schemaName, final String treeName);

    TreeCache populateTreeCache(TreeLink link) throws PersistitException;

    void flushAll() throws Exception;
}
