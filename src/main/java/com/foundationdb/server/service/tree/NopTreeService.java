/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.session.Session;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;

public class NopTreeService implements TreeService, Service
{
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

    @Override
    public Persistit getDb() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Exchange getExchange(Session session, TreeLink context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Exchange getExchange(Session session, Tree tree) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Key getKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseExchange(Session session, Exchange exchange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction getTransaction(Session session) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void visitStorage(Session session, TreeVisitor visitor, String treeName) throws PersistitException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isContainer(Exchange exchange, TreeLink storageLink) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkpoint() throws PersistitException {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableStatusCache getTableStatusCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TreeLink treeLink(String schemaName, String treeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDataPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String volumeForTree(String schemaName, String treeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean treeExists(String schemaName, String treeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TreeCache populateTreeCache(TreeLink link) throws PersistitException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flushAll() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Key createKey() {
        return new Key((Persistit)null);
    }
}
