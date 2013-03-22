
package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.FullTextQueryParseException;
import com.akiban.server.service.session.Session;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class FullTextIndexInfosImpl implements FullTextIndexInfos
{
    protected Map<IndexName,FullTextIndexShared> indexes = new HashMap<>();
    
    @Override
    public Query parseQuery(QueryContext context, IndexName name, 
                            String defaultField, String query) {
        FullTextIndexInfo index = getIndex(context.getSession(), name);
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
        FullTextIndexInfo index = getIndex(session, name);
        return index.getHKeyRowType();
    }

    protected abstract AkibanInformationSchema getAIS(Session session);
    protected abstract File getIndexPath();

    protected FullTextIndexInfo getIndex(Session session, IndexName name) {
        AkibanInformationSchema ais = getAIS(session);
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
