/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.tree;

import com.akiban.server.TableStatusCache;
import com.akiban.server.service.Service;
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
public interface TreeService extends Service<TreeService> {

    final static int MAX_TABLES_PER_VOLUME = 100000;

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String TREESPACE = "treespace";

    final static String SCHEMA = "schema";

    final static String TREE = "tree";

    Persistit getDb();

    Exchange getExchange(Session session, TreeLink context);

    Exchange getExchange(Session session, Tree tree);

    Key getKey(Session session);

    void releaseExchange(Session session, Exchange exchange);

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
