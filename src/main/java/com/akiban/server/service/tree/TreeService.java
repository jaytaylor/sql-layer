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

import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.persistit.*;
import com.persistit.exception.PersistitException;

/**
 * An abstraction for a layer that stores and retrieves data
 * 
 * @author peter
 * 
 */
public interface TreeService extends Service<TreeService> {

    final static int AIS_BASE_TABLE_ID = 1000000000;

    final static int MAX_TABLES_PER_VOLUME = 100000;

    final static String SCHEMA_TREE_NAME = "_schema_";

    final static String STATUS_TREE_NAME = "_status_";

    final static String TREESPACE="treespace";
    
    final static String SCHEMA = "schema";
    
    final static String TREE = "tree";

    Persistit getDb();

    Exchange getExchange(Session session, TreeLink context)
            throws PersistitException;

    Exchange getExchange(Session session, Tree tree) throws PersistitException;

    Key getKey(Session session) throws PersistitException;

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
