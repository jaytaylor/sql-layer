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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.FullTextQueryParseException;
import com.foundationdb.server.service.session.Session;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public abstract class FullTextIndexInfosImpl implements FullTextIndexInfos
{
    protected final Map<IndexName,FullTextIndexShared> indexes = new HashMap<>();
    
    @Override
    public Query parseQuery(QueryContext context, IndexName name, 
                            String defaultField, String query) {
        FullTextIndexInfo index = getIndex(context.getSession(), name, null);
        if (defaultField == null) {
            defaultField = index.getDefaultFieldName();
        }
        StandardQueryParser parser = index.getParser();
        try {
            synchronized (parser) {
                return parser.parse(query, defaultField);
            }
        }
        catch (QueryNodeException ex) {
            throw new FullTextQueryParseException(ex);
        }
    }

    @Override
    public RowType searchRowType(Session session, IndexName name) {
        FullTextIndexInfo index = getIndex(session, name, null);
        return index.getHKeyRowType();
    }

    protected abstract AkibanInformationSchema getAIS(Session session);
    protected abstract File getIndexPath();

    protected FullTextIndexInfo getIndexIfExists(Session session, IndexName name, AkibanInformationSchema ais) {
        if (ais == null)
            ais = getAIS(session);
        FullTextIndexInfo info = null;
        synchronized (indexes) {
            FullTextIndexShared shared = indexes.get(name);
            if (shared != null) {
                info = shared.forAIS(ais);
            }
        }
        return info;
    }

    protected FullTextIndexInfo getIndex(Session session, IndexName name, AkibanInformationSchema ais) {
        if (ais == null)
            ais = getAIS(session);
        FullTextIndexInfo info;
        synchronized (indexes) {
            FullTextIndexShared shared = indexes.get(name);
            if (shared != null) {
                info = shared.forAIS(ais);
            }
            else {
                shared = new FullTextIndexShared(name);
                info = new FullTextIndexInfo(shared);
                info.init(ais);
                info = shared.init(ais, info, getIndexPath());
                indexes.put(name, shared);
            }
        }
        return info;
    }
}
