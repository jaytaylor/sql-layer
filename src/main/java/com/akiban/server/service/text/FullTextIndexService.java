
package com.akiban.server.service.text;

import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;

import org.apache.lucene.search.Query;

/** Full service that does index maintenance and querying. */
public interface FullTextIndexService extends FullTextIndexInfos {
    public long createIndex(Session session, IndexName name);
    public void dropIndex(Session session, IndexName name);
    public Cursor searchIndex(QueryContext context, IndexName name, 
                              Query query, int limit);
}
