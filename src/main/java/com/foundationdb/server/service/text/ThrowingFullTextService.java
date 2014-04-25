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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.google.inject.Inject;
import org.apache.lucene.search.Query;

import java.util.Collection;

public class ThrowingFullTextService implements Service, FullTextIndexService, TableListener
{
    private static final RuntimeException EX = new UnsupportedSQLException("FULL_TEXT indexing not supported");

    private final ListenerService listenerService;

    @Inject
    public ThrowingFullTextService(ListenerService listenerService) {
        this.listenerService = listenerService;
    }


    //
    // Service
    //

    @Override
    public void start() {
        listenerService.registerTableListener(this);
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // FullTextIndexService
    //

    @Override
    public RowCursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        throw EX;
    }

    @Override
    public void backgroundWait() {
        throw EX;
    }

    @Override
    public Query parseQuery(QueryContext context, IndexName name, String defaultField, String query) {
        throw EX;
    }

    @Override
    public RowType searchRowType(Session session, IndexName name) {
        throw EX;
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
        if(!table.getFullTextIndexes().isEmpty()) {
            throw EX;
        }
    }

    @Override
    public void onDrop(Session session, Table table) {
        // None
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
        // NOne
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        for(Index i : indexes) {
            if(i.getIndexType() == IndexType.FULL_TEXT) {
                throw EX;
            }
        }
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
        // None
    }
}
