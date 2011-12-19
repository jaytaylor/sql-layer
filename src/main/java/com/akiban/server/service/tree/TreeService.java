/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
import com.persistit.exception.PersistitInterruptedException;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface TreeService extends Service<TreeService> {

    final static int MAX_TABLES_PER_VOLUME = 100000;

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String STATUS_TREE_NAME = "_status_";

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
}
