/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.service.tree;

import com.foundationdb.server.TableStatusCache;
import com.foundationdb.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

import java.util.Collection;

/** Service responsible for managing a Persistit instance and associated Trees. */
public interface TreeService
{
    Persistit getDb();

    Collection<String> getAllTreeNames();

    void treeWasRemoved(Session session, TreeLink link);

    Exchange getExchange(Session session, TreeLink context);

    Exchange getExchange(Session session, Tree tree);

    void releaseExchange(Session session, Exchange exchange);

    Transaction getTransaction(Session session);

    void visitStorage(Session session, TreeVisitor visitor, String treeName) throws PersistitException;

    TableStatusCache getTableStatusCache();

    TreeLink treeLink(String treeName);

    boolean treeExists(String treeName);

    Tree populateTreeCache(TreeLink link) throws PersistitException;
}
