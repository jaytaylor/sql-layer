
package com.akiban.server.service.text;

import com.akiban.ais.model.IndexName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.session.Session;

import org.apache.lucene.search.Query;

/** Subset of service that does build-time operations.
 * Also stubbed out for testing.
 */
public interface FullTextIndexInfos {
    public Query parseQuery(QueryContext context, IndexName name, 
                            String defaultField, String query);
    public RowType searchRowType(Session session, IndexName name);
}
